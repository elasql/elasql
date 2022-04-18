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
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.SequencerAnalyzer;
import org.elasql.schedule.calvin.StandardAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.ManuallyAbortException;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper> extends StoredProcedure<H> {
	private static Logger logger = Logger.getLogger(CalvinStoredProcedure.class.getName());

	// Protected resource
	protected long txNum;
	protected H paramHelper;
	protected CalvinCacheMgr cacheMgr;

	private ExecutionPlan execPlan;
	private Transaction tx;
	private boolean isCommitted = false;

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		super(paramHelper);

		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored procedure.
	 * Use the given {@link ReadWriteSetAnalyzer} to add keys.
	 * 
	 * @param analyzer an object that stores the read-set and the write-set
	 */
	protected abstract void prepareKeys(ReadWriteSetAnalyzer analyzer);

	protected abstract void executeSql(Map<PrimaryKey, CachedRecord> readings);

	/**********************
	 * implemented methods
	 **********************/

	public void prepare(Object... pars) {
//		Timer timer = Timer.getLocalTimer();

//		timer.startComponentTimer(getClass().getSimpleName() + " analyze paramters");
		execPlan = analyzeParameters(pars);
//		timer.stopComponentTimer(getClass().getSimpleName() + " analyze paramters");

		// The sequencer only analyzes the parameters
		if (Elasql.isStandAloneSequencer()) {
			return;
		}

		// Prepare a transaction and a cache
//		timer.startComponentTimer(getClass().getSimpleName() + " init transaction");
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		if (isParticipating()) {
			// create a transaction
			tx = Elasql.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, execPlan.isReadOnly(), txNum);
			tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));

			// create a cache manager
			cacheMgr = postOffice.createCacheMgr(tx, execPlan.hasRemoteReads());

			// For special transactions
			executeLogicInScheduler(tx);
		} else {
			postOffice.skipTransaction(txNum);
		}
//		timer.stopComponentTimer(getClass().getSimpleName() + " init transaction");

		// Debug
//		if (txNum % 500 == 1)
//			System.out.println(String.format("Tx.%d params: %s", txNum, Arrays.toString(pars)));
//			System.out.println("Tx." + txNum + "'s execution plan:\n" + execPlan);

		// Debug
//		if (Elasql.migrationMgr().isInMigration())
//			System.out.println("Tx." + txNum + "'s execution plan:\n" + execPlan);
	}

	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// analyze read-write set
		ReadWriteSetAnalyzer analyzer;
		if (Elasql.isStandAloneSequencer()) {
			// The sequencer monitors transactions
			SequencerAnalyzer seqAnalyzer = new SequencerAnalyzer();
			prepareKeys(seqAnalyzer);
			Elasql.migraSysControl().monitorTransaction(seqAnalyzer.getReadKeys(), seqAnalyzer.getWriteKeys());
			analyzer = seqAnalyzer;
		} else {
			if (Elasql.migrationMgr().isInMigration())
				analyzer = Elasql.migrationMgr().newAnalyzer();
			else
				analyzer = new StandardAnalyzer();
			prepareKeys(analyzer);
		}

		// generate execution plan
		return analyzer.generatePlan();
	}

	protected void executeLogicInScheduler(Transaction tx) {
		// Prepare for some special transactions (e.g. migration transactions)
	}

	public void bookConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		ccMgr.bookReadKeys(execPlan.getLocalReadKeys());
		ccMgr.bookReadKeys(execPlan.getLocalReadsForMigration());
		ccMgr.bookWriteKeys(execPlan.getLocalUpdateKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalInsertKeys());
		ccMgr.bookWriteKeys(execPlan.getLocalDeleteKeys());
		ccMgr.bookWriteKeys(execPlan.getIncomingMigratingKeys());
	}

	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		ccMgr.requestLocks();
	}

	@Override
	public SpResultSet execute() {
		try {
			// Get conservative locks it has asked before
//			Timer.getLocalTimer().startComponentTimer("get lock");
			getConservativeLocks();
//			Timer.getLocalTimer().stopComponentTimer("get lock");

			// Perform foreground migration
//			Timer.getLocalTimer().startComponentTimer("perform fg push");
			performForegroundMigration();
//			Timer.getLocalTimer().stopComponentTimer("perform fg push");

			// Execute transaction
			executeTransactionLogic();

			// Flush the cached records
//			Timer.getLocalTimer().startComponentTimer("flush");
			cacheMgr.flush();
//			Timer.getLocalTimer().stopComponentTimer("flush");

			// The transaction finishes normally
//			Timer.getLocalTimer().startComponentTimer("commit");
			tx.commit();
//			Timer.getLocalTimer().stopComponentTimer("commit");
			isCommitted = true;

			afterCommit();

		} catch (ManuallyAbortException me) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Manually aborted by the procedure: " + me.getMessage());
			tx.rollback();
		} catch (Exception e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("Tx." + txNum + " crashes. The execution plan: \n" + execPlan);
			e.printStackTrace();
			tx.rollback();
		} finally {
			// Clean the cache
			cacheMgr.notifyTxCommitted();
		}

		return new SpResultSet(isCommitted, paramHelper.getResultSetSchema(), paramHelper.newResultSetRecord());
	}

	public boolean isParticipating() {
		return execPlan.getParticipantRole() != ParticipantRole.IGNORE;
	}

	public boolean willResponseToClients() {
		return execPlan.getParticipantRole() == ParticipantRole.ACTIVE;
	}

	public boolean isReadOnly() {
		return execPlan.isReadOnly();
	}

	@Override
	protected void executeSql() {
		// Do nothing
		// Because we have overrided execute(), there is no need
		// to implement this method.
	}

	protected void performForegroundMigration() {
		// Sends/Handles the pull requests
		if (execPlan.isPullingMigration()) {
			sendMigrationPullRequests(execPlan.getPullingSources());
			waitForMigrationPullRequests(execPlan.getMigrationPushSets().keySet());
		}

		// Read the migrating records
		Map<PrimaryKey, CachedRecord> migratingRecs = performLocalRead(execPlan.getLocalReadsForMigration());

		// Push migrating records
		pushRecordsToRemotes(execPlan.getMigrationPushSets(), migratingRecs);

		// Wait for migrating records
		collectRemoteReadings(execPlan.getIncomingMigratingKeys(), migratingRecs);

		// Inserts the migrating records to the local storage
		performInsertionForMigrations(execPlan.getIncomingMigratingKeys(), migratingRecs);
	}

	/**
	 * This method will be called by execute(). The default implementation of this
	 * method follows the steps described by Calvin paper.
	 */
	protected void executeTransactionLogic() {
		// Read the local records
//		Timer.getLocalTimer().startComponentTimer("read local");
		Map<PrimaryKey, CachedRecord> readings = performLocalRead(execPlan.getLocalReadKeys());
//		Timer.getLocalTimer().stopComponentTimer("read local");

		// Push local records to the needed remote nodes
//		Timer.getLocalTimer().startComponentTimer("push reads");
		pushRecordsToRemotes(execPlan.getPushSets(), readings);
//		Timer.getLocalTimer().stopComponentTimer("push reads");

		// Passive participants stops here
		if (execPlan.getParticipantRole() != ParticipantRole.ACTIVE)
			return;

		// Read the remote records
//		Timer.getLocalTimer().startComponentTimer("read remote");
		collectRemoteReadings(execPlan.getRemoteReadKeys(), readings);
//		Timer.getLocalTimer().stopComponentTimer("read remote");

		// Write the local records
//		Timer.getLocalTimer().startComponentTimer("write local");
		executeSql(readings);
//		Timer.getLocalTimer().stopComponentTimer("write local");
	}

	protected void afterCommit() {
		// Used for clean up or notification.
	}

	protected void update(PrimaryKey key, CachedRecord rec) {
		if (execPlan.isLocalUpdate(key))
			cacheMgr.update(key, rec);
	}

	protected void insert(PrimaryKey key, Map<String, Constant> fldVals) {
		if (execPlan.isLocalInsert(key))
			cacheMgr.insert(key, fldVals);
	}

	protected void delete(PrimaryKey key) {
		if (execPlan.isLocalDelete(key))
			cacheMgr.delete(key);
	}

	protected Transaction getTransaction() {
		return tx;
	}

	protected void sendMigrationPullRequests(Set<Integer> targetNodes) {
		for (Integer nodeId : targetNodes) {
			// Construct pushing tuple set
			TupleSet ts = new TupleSet(-1);
			PrimaryKey key = NotificationPartitionPlan.createRecordKey(Elasql.serverId(), nodeId);
			CachedRecord dummyRec = NotificationPartitionPlan.createRecord(Elasql.serverId(), nodeId, txNum);
			ts.addTuple(key, txNum, txNum, dummyRec);

			// Push to the target
			Elasql.connectionMgr().pushTupleSet(nodeId, ts);
		}
	}

	protected void waitForMigrationPullRequests(Set<Integer> targetNodes) {
		for (Integer nodeId : targetNodes) {
			PrimaryKey key = NotificationPartitionPlan.createRecordKey(nodeId, Elasql.serverId());
			CachedRecord rec = cacheMgr.readFromRemote(key);
			if (rec.getSrcTxNum() != txNum || rec == null)
				throw new RuntimeException("something wrong with the pull request: " + key);
		}
	}

	private Map<PrimaryKey, CachedRecord> performLocalRead(Set<PrimaryKey> readKeys) {
		Map<PrimaryKey, CachedRecord> localReadings = new HashMap<PrimaryKey, CachedRecord>();

		// Read local records (for both active or passive participants)
		for (PrimaryKey k : readKeys) {
			CachedRecord rec = cacheMgr.readFromLocal(k);
			if (rec == null)
				throw new RuntimeException("cannot find the record for " + k + " in the local stroage");
			localReadings.put(k, rec);
		}

		return localReadings;
	}

	private void pushRecordsToRemotes(Map<Integer, Set<PrimaryKey>> pushKeys, Map<PrimaryKey, CachedRecord> records) {
		for (Map.Entry<Integer, Set<PrimaryKey>> entry : pushKeys.entrySet()) {
			Integer targetNodeId = entry.getKey();
			Set<PrimaryKey> keys = entry.getValue();

			// Construct pushing tuple set
			TupleSet ts = new TupleSet(-1);
			for (PrimaryKey key : keys) {
				CachedRecord rec = records.get(key);
				if (rec == null)
					throw new RuntimeException("cannot find the record for " + key);
				ts.addTuple(key, txNum, txNum, rec);
			}

			// Push to the target
			Elasql.connectionMgr().pushTupleSet(targetNodeId, ts);
		}
	}

	private void collectRemoteReadings(Set<PrimaryKey> keys, Map<PrimaryKey, CachedRecord> readingCache) {
		// Read remote records
		for (PrimaryKey k : keys) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			readingCache.put(k, rec);
		}
	}

	private void performInsertionForMigrations(Set<PrimaryKey> migratingKeys,
			Map<PrimaryKey, CachedRecord> migratingRecords) {
		for (PrimaryKey key : migratingKeys) {
			cacheMgr.insert(key, migratingRecords.get(key));
		}
	}
}
