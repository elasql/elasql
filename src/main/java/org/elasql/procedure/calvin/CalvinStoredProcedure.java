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
package org.elasql.procedure.calvin;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.procedure.DdStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.StandardAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper>
		implements DdStoredProcedure {
	private static Logger logger = Logger.getLogger(CalvinStoredProcedure.class.getName());

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;
	protected CalvinCacheMgr cacheMgr;
	protected ExecutionPlan execPlan;

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	protected abstract void prepareKeys(ReadWriteSetAnalyzer analyzer);

	protected abstract void executeSql(Map<RecordKey, CachedRecord> readings);

	/**********************
	 * implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// analyze read-write set
		ReadWriteSetAnalyzer analyzer;
		if (Elasql.migrationMgr().isInMigration())
			analyzer = Elasql.migrationMgr().newAnalyzer();
		else
			analyzer = new StandardAnalyzer();
		prepareKeys(analyzer);
		
		// generate execution plan
		execPlan = analyzer.generatePlan();
		
		// Debug
//		if (Elasql.migrationMgr().isInMigration()) {
//			System.out.println("Tx." + txNum + "'s execution plan:\n" + execPlan);
//		}
		
		// create a transaction
		tx = Elasql.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, execPlan.isReadOnly(), txNum);
		tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));
		
		// for the cache layer
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		if (isParticipated()) {
			// create a cache manager
			cacheMgr = postOffice.createCacheMgr(tx, execPlan.hasRemoteReads());
		} else {
			postOffice.skipTransaction(txNum);
		}
	}

	public void bookConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		ccMgr.bookReadKeys(execPlan.getLocalReadKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalUpdateKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalInsertKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalDeleteKeys());
		ccMgr.bookWriteKeys(execPlan.getInsertsForMigration());
	}

	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		ccMgr.requestLocks();
	}

	@Override
	public SpResultSet execute() {
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();
			
			// Execute transaction
			executeTransactionLogic();
			
			// Flush the cached records
			cacheMgr.flush();
			
			// The transaction finishes normally
			tx.commit();
			paramHelper.setCommitted(true);
			
			afterCommit();
			
		} catch (Exception e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("Tx." + txNum + " crashes. The execution plan: " + execPlan + paramHelper.getClass().getSimpleName());
			e.printStackTrace();
			tx.rollback();
			paramHelper.setCommitted(false);
		} finally {
			// Clean the cache
			cacheMgr.notifyTxCommitted();
		}

		return paramHelper.createResultSet();
	}

	public boolean isParticipated() {
		return execPlan.getParticipantRole() != ParticipantRole.IGNORE;
	}
	
	public boolean willResponseToClients() {
		return execPlan.getParticipantRole() == ParticipantRole.ACTIVE;
	}

	@Override
	public boolean isReadOnly() {
		return execPlan.isReadOnly();
	}

	/**
	 * This method will be called by execute(). The default implementation of
	 * this method follows the steps described by Calvin paper.
	 */
	protected void executeTransactionLogic() {
		// Read the local records
		Map<RecordKey, CachedRecord> readings = performLocalRead();

		// Push local records to the needed remote nodes
		pushReadingsToRemotes(readings);
		
		// Passive participants stops here
		if (execPlan.getParticipantRole() != ParticipantRole.ACTIVE)
			return;

		// Read the remote records
		collectRemoteReadings(readings);

		// Write the local records
		executeSql(readings);
		
		// perform insertions for migrations (if there is any)
		performInsertionForMigrations(readings);
	}
	
	protected void afterCommit() {
		// Used for clean up or notification.
	}
	
	protected void update(RecordKey key, CachedRecord rec) {
		if (execPlan.isLocalUpdate(key))
			cacheMgr.update(key, rec);
	}
	
	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		if (execPlan.isLocalInsert(key))
			cacheMgr.insert(key, fldVals);
	}
	
	protected void delete(RecordKey key) {
		if (execPlan.isLocalDelete(key))
			cacheMgr.delete(key);
	}

	private Map<RecordKey, CachedRecord> performLocalRead() {
		Map<RecordKey, CachedRecord> localReadings = new HashMap<RecordKey, CachedRecord>();
		
		// Read local records (for both active or passive participants)
		for (RecordKey k : execPlan.getLocalReadKeys()) {
			CachedRecord rec = cacheMgr.readFromLocal(k);
			if (rec == null)
				throw new RuntimeException("cannot find the record for " + k + " in the local stroage");
			localReadings.put(k, rec);
		}
		
		return localReadings;
	}

	private void pushReadingsToRemotes(Map<RecordKey, CachedRecord> readings) {
		for (Map.Entry<Integer, Set<RecordKey>> entry : execPlan.getPushSets().entrySet()) {
			Integer targetNodeId = entry.getKey();
			Set<RecordKey> keys = entry.getValue();
			
			// Construct pushing tuple set
			TupleSet ts = new TupleSet(-1);
			for (RecordKey key : keys) {
				CachedRecord rec = readings.get(key);
				if (rec == null)
					throw new RuntimeException("cannot find the record for " + key);
				ts.addTuple(key, txNum, txNum, readings.get(key));
			}
			
			// Push to the target
			Elasql.connectionMgr().pushTupleSet(targetNodeId, ts);
		}
	}

	private void collectRemoteReadings(Map<RecordKey, CachedRecord> readingCache) {
		// Read remote records
		for (RecordKey k : execPlan.getRemoteReadKeys()) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			readingCache.put(k, rec);
		}
	}
	
	private void performInsertionForMigrations(Map<RecordKey, CachedRecord> readings) {
		for (RecordKey key : execPlan.getInsertsForMigration()) {
			cacheMgr.insert(key, readings.get(key).getFldValMap());
		}
	}
}
