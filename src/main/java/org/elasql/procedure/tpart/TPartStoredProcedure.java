package org.elasql.procedure.tpart;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.tpart.CachedEntryKey;
import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.cache.tpart.TPartTxLocalCache;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.tpart.sink.PushInfo;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.DiskIOCounter;
import org.vanilladb.core.util.ThreadMXBean;
import org.vanilladb.core.util.Timer;

public abstract class TPartStoredProcedure<H extends StoredProcedureParamHelper>
		extends StoredProcedure<H> {
	
	public static enum ProcedureType {
		NOP, NORMAL, UTILITY, MIGRATION
	}

	// Protected resource
	protected long txNum;
	protected H paramHelper;
	protected int localNodeId;
	protected Transaction tx;

	// Private resource
	private Set<PrimaryKey> readKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> writeKeys = new HashSet<PrimaryKey>();
	private SunkPlan plan;
	private TPartTxLocalCache cache;
	private List<CachedEntryKey> cachedEntrySet = new ArrayList<CachedEntryKey>();
	private boolean isCommitted = false;

	public TPartStoredProcedure(long txNum, H paramHelper) {
		super(paramHelper);
		
		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");

		this.txNum = txNum;
		this.paramHelper = paramHelper;
		this.localNodeId = Elasql.serverId();
	}

	public abstract double getWeight();

	protected abstract void prepareKeys();

	protected abstract void executeSql(Map<PrimaryKey, CachedRecord> readings);

	@Override
	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// prepare keys
		prepareKeys();
	}

	public void decideExceutionPlan(SunkPlan p) {
		if (plan != null)
			throw new RuntimeException("The execution plan has been set");
		
		// Set plan
		plan = p;
		
		// create a transaction
		tx = Elasql.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, plan.isReadOnly(), txNum);
		tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));

		// create a local cache
		cache = new TPartTxLocalCache(tx);
		
		// register locks
		bookConservativeLocks();
	}

	public void bookConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		
		ccMgr.bookReadKeys(plan.getSinkReadingInfo());
		for (Set<PushInfo> infos : plan.getSinkPushingInfo().values())
			for (PushInfo info : infos)
				ccMgr.bookReadKey(info.getRecord());
		ccMgr.bookWriteKeys(plan.getLocalWriteBackInfo());
		ccMgr.bookWriteKeys(plan.getCacheDeletions());
	}


	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		
		ccMgr.requestLocks();
	}

	@Override
	public SpResultSet execute() {
		Timer timer = Timer.getLocalTimer();
		Timer cpuTimer = Timer.getLocalCpuTimer();
		DiskIOCounter ioCounter = DiskIOCounter.getLocalIOCounter();
		try {
			timer.startComponentTimer("Get locks");
			cpuTimer.startComponentTimer("Get locks", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("Get locks");
			getConservativeLocks();
			timer.stopComponentTimer("Get locks");
			cpuTimer.stopComponentTimer("Get locks", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("Get locks");
			
			executeTransactionLogic();
			
			timer.startComponentTimer("Tx commit");
			cpuTimer.startComponentTimer("Tx commit", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("Tx commit");
			tx.commit();
			timer.stopComponentTimer("Tx commit");
			cpuTimer.stopComponentTimer("Tx commit", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("Tx commit");
			
			isCommitted = true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Tx." + txNum + "'s plan: " + plan);
			timer.startComponentTimer("Tx rollback");
			cpuTimer.startComponentTimer("Tx rollback", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("Tx rollback");
			tx.rollback();
			timer.stopComponentTimer("Tx rollback");
			cpuTimer.stopComponentTimer("Tx rollback", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("Tx rollback");
		}
		return new SpResultSet(
			isCommitted,
			paramHelper.getResultSetSchema(),
			paramHelper.newResultSetRecord()
		);
	}
	
	@Override
	protected void executeSql() {
		// Do nothing
		// Because we have overrided execute(), there is no need
		// to implement this method.
	}

	public boolean isMaster() {
		return plan.isHereMaster();
	}

	public ProcedureType getProcedureType() {
		return ProcedureType.NORMAL;
	}

	public Set<PrimaryKey> getReadSet() {
		return readKeys;
	}

	public Set<PrimaryKey> getWriteSet() {
		return writeKeys;
	}
	
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public SunkPlan getSunkPlan() {
		return plan;
	}

	protected void addReadKey(PrimaryKey readKey) {
		readKeys.add(readKey);
	}

	protected void addWriteKey(PrimaryKey writeKey) {
		writeKeys.add(writeKey);
	}

	protected void addInsertKey(PrimaryKey insertKey) {
		writeKeys.add(insertKey);
	}

	protected void update(PrimaryKey key, CachedRecord rec) {
		cache.update(key, rec);
	}

	protected void insert(PrimaryKey key, Map<String, Constant> fldVals) {
		cache.insert(key, fldVals);
	}

	protected void delete(PrimaryKey key) {
		cache.delete(key);
	}

	private void executeTransactionLogic() {
		int sinkId = plan.sinkProcessId();
		Timer timer = Timer.getLocalTimer();
		Timer cpuTimer = Timer.getLocalCpuTimer();
		DiskIOCounter ioCounter = DiskIOCounter.getLocalIOCounter();

		if (plan.isHereMaster()) {
			Map<PrimaryKey, CachedRecord> readings = new HashMap<PrimaryKey, CachedRecord>();

			// Read the records from the local sink
			timer.startComponentTimer("(Master) Read from local storage");
			cpuTimer.startComponentTimer("(Master) Read from local storage", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("(Master) Read from local storage");
			for (PrimaryKey k : plan.getSinkReadingInfo()) {
				readings.put(k, cache.readFromSink(k));
			}
			timer.stopComponentTimer("(Master) Read from local storage");
			cpuTimer.stopComponentTimer("(Master) Read from local storage", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("(Master) Read from local storage");
			
			// Read all needed records
			timer.startComponentTimer("(Master) Read from remote");
			cpuTimer.startComponentTimer("(Master) Read from remote", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("(Master) Read from remote");
			for (PrimaryKey k : plan.getReadSet()) {
				if (!readings.containsKey(k)) {
					long srcTxNum = plan.getReadSrcTxNum(k);
					readings.put(k, cache.read(k, srcTxNum));
					cachedEntrySet.add(new CachedEntryKey(k, srcTxNum, txNum));
				}
			}
			timer.stopComponentTimer("(Master) Read from remote");
			cpuTimer.stopComponentTimer("(Master) Read from remote", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("(Master) Read from remote");
			
			// Execute the SQLs defined by users
			timer.startComponentTimer("(Master) Execute SQL");
			cpuTimer.startComponentTimer("(Master) Execute SQL", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("(Master) Execute SQL");
			executeSql(readings);
			timer.stopComponentTimer("(Master) Execute SQL");
			cpuTimer.stopComponentTimer("(Master) Execute SQL", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("(Master) Execute SQL");
			
			// Push the data to where they need at
			timer.startComponentTimer("(Master) Push");
			cpuTimer.startComponentTimer("(Master) Push", ThreadMXBean.getCpuTime());
			ioCounter.startComponentCounter("(Master) Push");
			Map<Integer, Set<PushInfo>> pi = plan.getPushingInfo();
			if (pi != null) {
				// read from local storage and send to remote site
				for (Entry<Integer, Set<PushInfo>> entry : pi.entrySet()) {
					int targetServerId = entry.getKey();

					// Construct a tuple set
					TupleSet rs = new TupleSet(sinkId);
					for (PushInfo pushInfo : entry.getValue()) {
						CachedRecord rec = cache.read(pushInfo.getRecord(), txNum);
						cachedEntrySet.add(new CachedEntryKey(pushInfo.getRecord(), txNum, pushInfo.getDestTxNum()));
						rs.addTuple(pushInfo.getRecord(), txNum, pushInfo.getDestTxNum(), rec);
					}

					// Push to the remote
					Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
				}
			}
			timer.stopComponentTimer("(Master) Push");
			cpuTimer.stopComponentTimer("(Master) Push", ThreadMXBean.getCpuTime());
			ioCounter.stopComponentCounter("(Master) Push");
		} else if (plan.hasSinkPush()) {
			long sinkTxnNum = TPartCacheMgr.toSinkId(Elasql.serverId());
			
			for (Entry<Integer, Set<PushInfo>> entry : plan.getSinkPushingInfo().entrySet()) {
				int targetServerId = entry.getKey();
				TupleSet rs = new TupleSet(sinkId);
				
				// Migration transactions
//				if (getProcedureType() == ProcedureType.MIGRATION) {
//					long destTxNum = -1;
//					
//					Set<RecordKey> keys = new HashSet<RecordKey>();
//					for (PushInfo pushInfo : entry.getValue()) {
//						keys.add(pushInfo.getRecord());
//						// XXX: Not good
//						if (destTxNum == -1)
//							destTxNum = pushInfo.getDestTxNum();
//					}
//					
//					Map<RecordKey, CachedRecord> recs = cache.batchReadFromSink(keys);
//					
//					for (Entry<RecordKey, CachedRecord> keyRecPair : recs.entrySet()) {
//						RecordKey key = keyRecPair.getKey();
//						CachedRecord rec = keyRecPair.getValue();
//						rec.setSrcTxNum(sinkTxnNum);
//						rs.addTuple(key, sinkTxnNum, destTxNum, rec);
//					}
//					
//				} else {
					// Normal transactions
					timer.startComponentTimer("(Slave) Read from local storage");
					cpuTimer.startComponentTimer("(Slave) Read from local storage", ThreadMXBean.getCpuTime());
					ioCounter.startComponentCounter("(Slave) Read from local storage");
					for (PushInfo pushInfo : entry.getValue()) {
						
						CachedRecord rec = cache.readFromSink(pushInfo.getRecord());
						// TODO deal with null value record
						rec.setSrcTxNum(sinkTxnNum);
						rs.addTuple(pushInfo.getRecord(), sinkTxnNum, pushInfo.getDestTxNum(), rec);
					}
					timer.stopComponentTimer("(Slave) Read from local storage");
					cpuTimer.stopComponentTimer("(Slave) Read from local storage", ThreadMXBean.getCpuTime());
					ioCounter.stopComponentCounter("(Slave) Read from local storage");
//				}

				timer.startComponentTimer("(Slave) Push");
				cpuTimer.startComponentTimer("(Slave) Push", ThreadMXBean.getCpuTime());
				ioCounter.startComponentCounter("(Slave) Push");
				Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
				timer.stopComponentTimer("(Slave) Push");
				cpuTimer.stopComponentTimer("(Slave) Push", ThreadMXBean.getCpuTime());
				ioCounter.stopComponentCounter("(Slave) Push");
			}
		}

		// Flush the cached data
		// including the writes to the next transaction and local write backs
		timer.startComponentTimer("Flush");
		cpuTimer.startComponentTimer("Flush", ThreadMXBean.getCpuTime());
		ioCounter.startComponentCounter("Flush");
		cache.flush(plan,  cachedEntrySet);
		timer.stopComponentTimer("Flush");
		cpuTimer.stopComponentTimer("Flush", ThreadMXBean.getCpuTime());
		ioCounter.stopComponentCounter("Flush");
	}
}
