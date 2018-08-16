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
import org.elasql.procedure.DdStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.tpart.sink.PushInfo;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class TPartStoredProcedure<H extends StoredProcedureParamHelper> implements DdStoredProcedure {

	public static enum ProcedureType {
		NOP, NORMAL, UTILITY, MIGRATION
	}

	// Protected resource
	protected long txNum;
	protected H paramHelper;
	protected int localNodeId;
	protected Transaction tx;

	// Private resource
	private Set<RecordKey> readKeys = new HashSet<RecordKey>();
	private Set<RecordKey> writeKeys = new HashSet<RecordKey>();
	private SunkPlan plan;
	private TPartTxLocalCache cache;
	private List<CachedEntryKey> cachedEntrySet = new ArrayList<CachedEntryKey>();

	public TPartStoredProcedure(long txNum, H paramHelper) {
		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");

		this.txNum = txNum;
		this.paramHelper = paramHelper;
		this.localNodeId = Elasql.serverId();
	}

	public abstract double getWeight();

	protected abstract void prepareKeys();

	protected abstract void executeSql(Map<RecordKey, CachedRecord> readings);

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
	}

	@Override
	public SpResultSet execute() {
		try {
			// Execute transaction
			executeTransactionLogic();

			tx.commit();
			paramHelper.setCommitted(true);
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			paramHelper.setCommitted(false);
		}
		return paramHelper.createResultSet();
	}

	public boolean isMaster() {
		return plan.isLocalTask();
	}

	public ProcedureType getProcedureType() {
		return ProcedureType.NORMAL;
	}

	public Set<RecordKey> getReadSet() {
		return readKeys;
	}

	public Set<RecordKey> getWriteSet() {
		return writeKeys;
	}

	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}
	
	public long getTxNum() {
		return txNum;
	}

	protected void addReadKey(RecordKey readKey) {
		readKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		writeKeys.add(writeKey);
	}

	protected void addInsertKey(RecordKey insertKey) {
		writeKeys.add(insertKey);
	}

	protected void update(RecordKey key, CachedRecord rec) {
		cache.update(key, rec);
	}

	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		cache.insert(key, fldVals);
	}

	protected void delete(RecordKey key) {
		cache.delete(key);
	}

	private void executeTransactionLogic() {
		int sinkId = plan.sinkProcessId();

		if (plan.isLocalTask()) {
			Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();
			//Timers.getTimer().startComponentTimer("read");
			// Read the records from the local sink
			for (RecordKey k : plan.getSinkReadingInfo()) {
				readings.put(k, cache.readFromSink(k));
			}

			// Read all needed records
			for (RecordKey k : plan.getReadSet()) {
				if (!readings.containsKey(k)) {
					long srcTxNum = plan.getReadSrcTxNum(k);
					readings.put(k, cache.read(k, srcTxNum));
					cachedEntrySet.add(new CachedEntryKey(k, srcTxNum, txNum));
				}
			}
			//Timers.getTimer().stopComponentTimer("read");
			// Execute the SQLs defined by users
			executeSql(readings);

			// Push the data to where they need at
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
					for (PushInfo pushInfo : entry.getValue()) {
						
						CachedRecord rec = cache.readFromSink(pushInfo.getRecord());
						// TODO deal with null value record
						rec.setSrcTxNum(sinkTxnNum);
						rs.addTuple(pushInfo.getRecord(), sinkTxnNum, pushInfo.getDestTxNum(), rec);
					}
//				}
				
				Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
			}
		}

		// Flush the cached data
		// including the writes to the next transaction and local write backs
		cache.flush(plan,  cachedEntrySet);
	}
}
