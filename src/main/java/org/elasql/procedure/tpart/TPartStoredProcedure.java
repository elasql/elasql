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
import org.vanilladb.core.util.TransactionProfiler;

public abstract class TPartStoredProcedure<H extends StoredProcedureParamHelper> extends StoredProcedure<H> {

	public static enum ProcedureType {
		NOP, NORMAL, UTILITY, MIGRATION, CONTROL
	}

	// Protected resource
	protected long txNum;
	protected H paramHelper;
	protected int localNodeId;
	protected Transaction tx;

	// Private resource
	private Set<PrimaryKey> readKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> updateKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> insertKeys = new HashSet<PrimaryKey>();

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
		
//		System.out.println(Thread.currentThread().getName() + " is booking the keys for tx " + txNum);
	}

	public void bookConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();

		ccMgr.bookReadKeys(plan.getSinkReadingInfo());
		for (Set<PushInfo> infos : plan.getSinkPushingInfo().values()) {
			for (PushInfo info : infos) {
				ccMgr.bookReadKey(info.getRecord());
			}
		}
		ccMgr.bookWriteKeys(plan.getLocalWriteBackInfo());
		ccMgr.bookWriteKeys(plan.getCacheDeletions());
	}

	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();

		ccMgr.requestLocks();
	}

	@Override
	public SpResultSet execute() {
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		try {
			profiler.setStageIndicator(3);
			profiler.startComponentProfiler("OU3 - Acquire Locks");
			getConservativeLocks();
			profiler.stopComponentProfiler("OU3 - Acquire Locks");
			profiler.resetStageIndicator();

			executeTransactionLogic();

			profiler.startComponentProfiler("OU8 - Commit");
			tx.commit();
			profiler.stopComponentProfiler("OU8 - Commit");

			isCommitted = true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Tx." + txNum + "'s plan: " + plan);
			profiler.startComponentProfiler("Tx rollback");
			tx.rollback();
			profiler.stopComponentProfiler("Tx rollback");
		}
		return new SpResultSet(isCommitted, paramHelper.getResultSetSchema(), paramHelper.newResultSetRecord());
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

	public boolean isTxDistributed() {
		return plan.isTxDistributed();
	}

	public ProcedureType getProcedureType() {
		return ProcedureType.NORMAL;
	}

	public Set<PrimaryKey> getReadSet() {
		return readKeys;
	}

	public Set<PrimaryKey> getUpdateSet() {
		return updateKeys;
	}

	public Set<PrimaryKey> getInsertSet() {
		return insertKeys;
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

	protected void addUpdateKey(PrimaryKey writeKey) {
		updateKeys.add(writeKey);
	}

	protected void addInsertKey(PrimaryKey insertKey) {
		insertKeys.add(insertKey);
	}

	protected void update(PrimaryKey key, CachedRecord rec) {
		cache.update(key, rec);
	}

	protected void insert(PrimaryKey key, Map<String, Constant> fldVals) {
		cache.insert(key, fldVals);
	}

	private void executeTransactionLogic() {
		int sinkId = plan.sinkProcessId();
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();

		if (plan.isHereMaster()) {
			Map<PrimaryKey, CachedRecord> readings = new HashMap<PrimaryKey, CachedRecord>();

			// Read the records from the local sink
			profiler.setStageIndicator(4);
			profiler.startComponentProfiler("OU4 - Read from Local");
			for (PrimaryKey k : plan.getSinkReadingInfo()) {
				readings.put(k, cache.readFromSink(k));
			}
			profiler.stopComponentProfiler("OU4 - Read from Local");
			profiler.resetStageIndicator();

			// Read all needed records
			profiler.startComponentProfiler("OU5M - Read from Remote");
			for (PrimaryKey k : plan.getReadSet()) {
				if (!readings.containsKey(k)) {
					long srcTxNum = plan.getReadSrcTxNum(k);
					readings.put(k, cache.read(k, srcTxNum));
					cachedEntrySet.add(new CachedEntryKey(k, srcTxNum, txNum));
				}
			}
			profiler.stopComponentProfiler("OU5M - Read from Remote");

			// Execute the SQLs defined by users
			profiler.startComponentProfiler("OU6 - Execute Arithmetic Logic");
			executeSql(readings);
			profiler.stopComponentProfiler("OU6 - Execute Arithmetic Logic");

			// Push the data to where they need at
			profiler.startComponentProfiler("NonOU - Push to Remote");
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
			profiler.stopComponentProfiler("NonOU - Push to Remote");
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
				profiler.startComponentProfiler("OU4 - Read from Local");
				for (PushInfo pushInfo : entry.getValue()) {

					CachedRecord rec = cache.readFromSink(pushInfo.getRecord());
					// TODO deal with null value record
					rec.setSrcTxNum(sinkTxnNum);
					rs.addTuple(pushInfo.getRecord(), sinkTxnNum, pushInfo.getDestTxNum(), rec);
				}
				profiler.stopComponentProfiler("OU4 - Read from Local");
//				}

				profiler.startComponentProfiler("OU5S - Push to Remote");
				Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
				profiler.stopComponentProfiler("OU5S - Push to Remote");
			}
		}

		// Flush the cached data
		// including the writes to the next transaction and local write backs
		profiler.setStageIndicator(7);
		profiler.startComponentProfiler("OU7 - Write to Local");
		cache.flush(plan, cachedEntrySet);
		profiler.stopComponentProfiler("OU7 - Write to Local");
		profiler.resetStageIndicator();
	}
}
