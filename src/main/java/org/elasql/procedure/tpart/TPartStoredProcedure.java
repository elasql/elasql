package org.elasql.procedure.tpart;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.tpart.CachedEntryKey;
import org.elasql.cache.tpart.TPartCacheMgr;
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
		NOP, NORMAL, UTILITY
	}

	// Protected resource
	protected long txNum;
	protected H paramHelper;
	protected int localNodeId;
	protected Transaction tx;

	// Private resource
	private Set<RecordKey> readKeys = new HashSet<RecordKey>();
	private Set<RecordKey> writeKeys = new HashSet<RecordKey>();
	private List<CachedEntryKey> cachedEntrySet = new ArrayList<CachedEntryKey>();
	private SunkPlan plan;

	private TPartCacheMgr cm = (TPartCacheMgr) Elasql.remoteRecReceiver();

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

		// create a transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		tx = Elasql.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));

		// prepare keys
		prepareKeys();
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

	public void setSunkPlan(SunkPlan p) {
		plan = p;
	}

	public SunkPlan getSunkPlan() {
		return plan;
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
		Long[] destTxNums = plan.getWritingDestOfRecord(key);
		cm.update(key, rec.getFldValMap(), tx, destTxNums);
	}

	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		Long[] destTxNums = plan.getWritingDestOfRecord(key);
		cm.insert(key, fldVals, tx, destTxNums);
	}

	protected void delete(RecordKey key) {
		Long[] destTxNums = plan.getWritingDestOfRecord(key);
		cm.delete(key, tx, destTxNums);
	}

	private void executeTransactionLogic() {
		int sinkId = plan.sinkProcessId();

		if (plan.isLocalTask()) {
			// Read the records from the local sink
			for (RecordKey k : plan.getSinkReadingInfo()) {
				cm.createCacheRecordFromSink(k, plan.sinkProcessId(), tx, txNum);
			}

			// Read all needed records
			Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();
			for (RecordKey k : readKeys) {
				long srcTxNum = plan.getReadSrcTxNum(k);
				readings.put(k, cm.read(k, srcTxNum, txNum));
				cachedEntrySet.add(new CachedEntryKey(k, srcTxNum, txNum));
			}

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
						CachedRecord rec = cm.read(pushInfo.getRecord(), txNum, pushInfo.getDestTxNum());
						cachedEntrySet.add(new CachedEntryKey(pushInfo.getRecord(), txNum, pushInfo.getDestTxNum()));
						rs.addTuple(pushInfo.getRecord(), txNum, pushInfo.getDestTxNum(), rec);
					}

					// Push to the remote
					Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
				}
			}
		} else if (plan.hasSinkPush()) {
			for (Entry<Integer, Set<PushInfo>> entry : plan.getSinkPushingInfo().entrySet()) {
				int targetServerId = entry.getKey();
				TupleSet rs = new TupleSet(sinkId);
				for (PushInfo pushInfo : entry.getValue()) {
					long sinkTxnNum = TPartCacheMgr.getPartitionTxnId(Elasql.serverId());
					CachedRecord rec = cm.readFromSink(pushInfo.getRecord(), sinkId, tx);
					// TODO deal with null value record
					rec.setSrcTxNum(sinkTxnNum);
					rs.addTuple(pushInfo.getRecord(), sinkTxnNum, pushInfo.getDestTxNum(), rec);
				}
				Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
			}
		}

		// Local write back
		if (plan.hasLocalWriteBack()) {
			for (RecordKey wk : plan.getLocalWriteBackInfo()) {
				cm.writeBack(wk, txNum, sinkId, tx);
			}
		}

		// Remove the cached data
		removeCachedRecords();
	}

	private void removeCachedRecords() {
		for (CachedEntryKey key : cachedEntrySet) {
			cm.uncache(key.getRecordKey(), key.getSource(), key.getDestination());
		}
	}
}
