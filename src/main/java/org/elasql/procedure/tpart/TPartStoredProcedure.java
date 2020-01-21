/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
import org.elasql.sql.RecordKey;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class TPartStoredProcedure<H extends StoredProcedureParamHelper>
		extends StoredProcedure<H> {

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

		// create a local cache
		cache = new TPartTxLocalCache(tx);
	}

	@Override
	public SpResultSet execute() {
		try {
			// Execute transaction
			executeTransactionLogic();

			tx.commit();
			isCommitted = true;
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
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

			// Read the records from the local sink
			for (RecordKey k : plan.getSinkReadingInfo()) {
				readings.put(k, cache.readFromSink(k));
			}

			// Read all needed records
			for (RecordKey k : readKeys) {
				if (!readings.containsKey(k)) {
					long srcTxNum = plan.getReadSrcTxNum(k);
					readings.put(k, cache.read(k, srcTxNum));
					cachedEntrySet.add(new CachedEntryKey(k, srcTxNum, txNum));
				}
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
						CachedRecord rec = readings.get(pushInfo.getRecord());
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
					long sinkTxnNum = TPartCacheMgr.toSinkId(Elasql.serverId());
					CachedRecord rec = cache.readFromSink(pushInfo.getRecord());
					// TODO deal with null value record
					rec.setSrcTxNum(sinkTxnNum);
					rs.addTuple(pushInfo.getRecord(), sinkTxnNum, pushInfo.getDestTxNum(), rec);
				}
				Elasql.connectionMgr().pushTupleSet(targetServerId, rs);
			}
		}

		// Flush the cached data
		// including the writes to the next transaction and local write backs
		cache.flush(plan,  cachedEntrySet);
	}
}
