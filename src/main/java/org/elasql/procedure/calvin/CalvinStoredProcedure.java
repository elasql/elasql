/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.procedure.calvin;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.procedure.DdStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper> implements DdStoredProcedure {

	// All New Design of Squll
	// Assumption :
	// 1) no blind write tx
	// 2) not supported insert

	// For simulating pull request
	private static final RecordKey PULL_REQUEST_KEY;
	private static final String DUMMY_FIELD1 = "dummy_field1";
	private static final String DUMMY_FIELD2 = "dummy_field2";

	static {
		// Create a pull key
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put(DUMMY_FIELD1, new IntegerConstant(0));
		PULL_REQUEST_KEY = new RecordKey("notification", keyEntryMap);
	}

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;
	protected int localNodeId;
	protected CalvinCacheMgr cacheMgr;

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private boolean isActiveParticipant, isPassiveParticipant;

	// For read-only transactions to choose one node as a active participant
	private int mostReadsNode = 0;
	private int[] readsPerNodes = new int[PartitionMetaMgr.NUM_PARTITIONS];

	// Record keys
	// XXX: Do we need table-level locks ?
	// XXX: We assume the fully replicated keys are read-only
	private Set<RecordKey> localReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> fullyRepKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localWriteKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localInsertKeys = new HashSet<RecordKey>();
	private Set<RecordKey> remoteReadKeys = new HashSet<RecordKey>();

	// Migration
	private MigrationManager migraMgr = Elasql.migrationMgr();
	private boolean isSourceNode;
	private boolean isDestNode;

	private Set<RecordKey> pullKeys = new HashSet<RecordKey>();
	// Not Migrated Readkeys
	// private Set<RecordKey> pullReadKeys = new HashSet<RecordKey>();
	// Not Migrated Writekeys
	// private Set<RecordKey> pullWriteKeys = new HashSet<RecordKey>();
	private Set<RecordKey> readKeysInMigration = new HashSet<RecordKey>();
	private Set<RecordKey> writeKeysInMigration = new HashSet<RecordKey>();

	private Set<RecordKey> SourceWriteKeys = new HashSet<RecordKey>();

	private Set<RecordKey> keysForBGPush = new HashSet<RecordKey>();
	protected boolean isExecutingInSrc = true;

	private boolean isInMigrating = false, isAnalyzing = false;
	private boolean activePulling = false;
	private boolean isMigrationTx = false;
	protected boolean isAsyncMigrateProc = false;
	private boolean someKeyMigrated = false;

	private boolean islog = false;
	private Map<RecordKey, CachedRecord> readings;

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;
		this.localNodeId = Elasql.serverId();
		// Becareful with this
		this.isSourceNode = (localNodeId == migraMgr.getSourcePartition());
		this.isDestNode = (localNodeId == migraMgr.getDestPartition());
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
	protected abstract void prepareKeys();

	protected abstract void executeSql(Map<RecordKey, CachedRecord> readings);

	/**********************
	 * implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// check if this transaction is in a migration period
		isInMigrating = migraMgr.isMigrating();
		isAnalyzing = migraMgr.isAnalyzing();

		// prepare parameters
		paramHelper.prepareParameters(pars);

		// prepare keys
		prepareKeys();

		// Stand Alone Analyzing
		if (!isInMigrating && isAnalyzing) {
			// Add the inserted keys to the candidates for BG pushes
			if (isSourceNode) {
				for (RecordKey key : keysForBGPush)
					migraMgr.addNewInsertKey(key);
			}
		}

		// Decide whether it is a migration tx :
		isMigrationTx = decideMigration();

		if (isMigrationTx) {

			// Decide to Do Migration

			// Transfer read/write/remote keys form Calvin to migrated

			// 1)Source part
			if (isSourceNode) {
				// Remove migrated ReadKeys from local read (Dest should local
				// read it)
				localReadKeys.removeAll(readKeysInMigration);
				// Remove migrated WriteKeys from local write(Dest should local
				// write it)
				localWriteKeys.removeAll(writeKeysInMigration);
				// Add migrated ReadKeys to Remote(Dest should pass to it)
				remoteReadKeys.addAll(readKeysInMigration);
			}
			// 2)Destination
			if (isDestNode) {
				// Add migrated ReadKeys to local read (Source will/already
				// migrate to it)
				localReadKeys.addAll(readKeysInMigration);
				// Add migrated Write to local write (Source will/already
				// migrate to it)
				localWriteKeys.addAll(writeKeysInMigration);
				// Remove migrated ReadKeys from Remote (Source will/already
				// migrate to it)
				remoteReadKeys.removeAll(readKeysInMigration);
				// Dest should have the pullKeys Write lock to prevent other
				// early access
				localWriteKeys.addAll(pullKeys);
			}

			// ActiveParticipants would be Dest and Src(if have write on it)
			// same as Calvin did

			// Set activeParticipants correctly
			// if Source have some local write that didn't in the migration
			// range, we execute tx at both side
			// All WriteKeys at destination are either already migrated or just
			// migrate by pulling

			// This means all WriteKey in source are in the migration range
			// Thus Soucre won't have any write keys and should be removed form
			// the activeParticipants
			if (!isSourceWriteKeysPartialNotInRange())
				activeParticipants.remove(migraMgr.getSourcePartition());

			// 1) This means some writeKeys in this tx will execute in Dest
			// So add Dest into activeParticipants, this is prevent that before
			// migration there is no writekeys on Dest

			// if (!writeKeysInMigration.isEmpty())
			// activeParticipants.add(migraMgr.getDestPartition());

			// 2) If this is a read only tx, deterministic add Dest to
			// activeParticipants
			// Always add Dest to activeParticipants
			activeParticipants.add(migraMgr.getDestPartition());

			// Deal with pull keys whether lunch pulling
			if (!pullKeys.isEmpty()) {
				// !Prevent ReadOnly but touch migration rage case
				// if(!localWriteKeys.isEmpty()&&)
				// activeParticipants.add(migraMgr.getSourcePartition());
				// Set all not migrated key to migrated
				migraMgr.setRecordMigrated(pullKeys);
				// Set pulling flag
				activePulling = true;
			}

			// Only Dest can respond client

			// Take care for AsyncPush
			if (isAsyncMigrateProc)
				activeParticipants.add(migraMgr.getSourcePartition());
			// Old code for insert
			//
			// // Add the inserted keys to the candidates for BG pushes
			// if (isExecutingInSrc && isSourceNode) {
			// for (RecordKey key : keysForBGPush)
			// migraMgr.addNewInsertKey(key);
			// }

		} else {
			// Calvin way decide activeParticipants
			// if there is no active participant (e.g. read-only transaction),
			// choose the one with most readings as the only active participant

			if (activeParticipants.isEmpty())
				activeParticipants.add(mostReadsNode);

		}

		// Decide the role
		if (activeParticipants.contains(localNodeId))
			isActiveParticipant = true;
		else if (!localReadKeys.isEmpty() || (!pullKeys.isEmpty() && isSourceNode))
			isPassiveParticipant = true;

		// create a transaction

		boolean isReadOnly = paramHelper.isReadOnly();

		if (isMigrationTx)
			isReadOnly = false;

		tx = Elasql.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));

		// for the cache layer
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		cacheMgr = postOffice.createCacheMgr(tx, true);

		/*
		 * if (isParticipated()) { cacheMgr = postOffice.createCacheMgr(tx,
		 * true); // create a cache manager /* if (remoteReadKeys.isEmpty()&&
		 * !(paramHelper instanceof AsyncMigrateParamHelper)) cacheMgr =
		 * postOffice.createCacheMgr(tx, false ); else cacheMgr =
		 * postOffice.createCacheMgr(tx, true);
		 */
		/*
		 * } else { postOffice.skipTransaction(txNum);
		 */

		if (islog && isInMigrating) {
			String str = "******\nisInMigrating : " + isInMigrating;
			str = str + "\n Txnum : " + txNum;
			str = str + "\n isMigrationTx : " + isMigrationTx;
			Class<?> enclosingClass = getClass().getEnclosingClass();
			if (enclosingClass != null) {
				str = str + "\n Classname : " + enclosingClass.getName();

			} else {
				str = str + "\n Classname : " + getClass().getName();

			}
			str = str + "\n readKeysInMigration : ";
			for (Integer k : recordKeyToSortArray(readKeysInMigration))
				str = str + " , " + k;
			str = str + "\n writeKeysInMigration : ";
			for (Integer k : recordKeyToSortArray(writeKeysInMigration))
				str = str + " , " + k;
			str = str + "\n Local Read : ";
			for (Integer k : recordKeyToSortArray(localReadKeys))
				str = str + " , " + k;
			str = str + "\n Local Write : ";
			for (Integer k : recordKeyToSortArray(localWriteKeys))
				str = str + " , " + k;
			str = str + "\n Remote Read : ";
			for (Integer k : recordKeyToSortArray(remoteReadKeys))
				str = str + " , " + k;
			str = str + "\n activeParticipants : " + activeParticipants;
			str = str + "\n isParticipated : " + isParticipated();
			str = str + "\n ReadNodes : ";
			for (int k : readsPerNodes)
				str = str + " , " + k;
			str = str + "\n pullKeys : ";
			for (Integer k : recordKeyToSortArray(pullKeys))
				str = str + " , " + k;
			str = str + "\n activePulling : " + activePulling;
			str = str + "\n ******";
			System.out.println(str);

		}
		/*
		 * if (!this.isSourceNode && islog) { String str = "********" +
		 * System.currentTimeMillis() + "\n Txnum : " + txNum; str = str +
		 * "\n isMigrationTx : " + isMigrationTx; str = str + "\n Local Read : "
		 * ; for (Integer k : recordKeyToSortArray(localReadKeys)) str = str +
		 * " , " + k; str = str + "\n Local Write : "; for (Integer k :
		 * recordKeyToSortArray(localWriteKeys)) str = str + " , " + k; str =
		 * str + "\n Remote Read : "; for (Integer k :
		 * recordKeyToSortArray(remoteReadKeys)) str = str + " , " + k; str =
		 * str + "\n activeParticipants : " + activeParticipants; str = str +
		 * "\n********"; System.out.println(str); }
		 */

	}

	public void bookConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();

		ccMgr.bookReadKeys(localReadKeys);
		ccMgr.bookWriteKeys(localWriteKeys);
		ccMgr.bookWriteKeys(localInsertKeys);
	}

	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();

		ccMgr.requestLocks(isInMigrating && islog);
	}

	@Override
	public SpResultSet execute() {
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();
			if (islog)
				System.out.println("Tx : " + txNum + " End of get lock!");
			// Execute transaction
			executeTransactionLogic();

			// Flush the cached records
			cacheMgr.flush();

			// The transaction finishes normally
			tx.commit();
			paramHelper.setCommitted(true);

			// Something might be done after committing
			afterCommit();
			if (islog)
				System.out.println("Tx : " + txNum + " Commited!");

		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			String str = "TX : " + txNum + "Abort cause Exception!\n";
			if (readings != null) {
				str = str + "\n Readings : ";
				for (Integer k : recordKeyToSortArray(readings.keySet()))
					str = str + " , " + k;
				str = str + "\n Error check : \n";
				for (Entry<RecordKey, CachedRecord> pair : readings.entrySet()) {
					if (pair.getValue() == null)
						str = str + "key" + pair.getKey() + "is null!\n";
					else if (pair.getValue().getVal("i_name") == null)
						str = str + "key" + pair.getKey() + "no map!\n";

				}
			}
			System.out.println(str + errors.toString());

			// e.printStackTrace();
			tx.rollback();

			paramHelper.setCommitted(false);
		} finally {

			cacheMgr.notifyTxCommitted();
		}

		return paramHelper.createResultSet();
	}

	public boolean isParticipated() {
		return isActiveParticipant || isPassiveParticipant;
	}

	public boolean willResponseToClients() {
		if (isMigrationTx)
			return isDestNode;
		else
			return isActiveParticipant;
	}

	private boolean isSourceWriteKeysPartialNotInRange() {
		for (RecordKey k : SourceWriteKeys)
			if (!migraMgr.keyIsInMigrationRange(k))
				return true;
		return false;
	}

	private boolean decideMigration() {

		// 0) isAsyncMigrateProc
		if (isAsyncMigrateProc)
			return true;
		// 1) isInMigrating
		if (!isInMigrating)
			return false;
		// 2) have keys in Migration range
		if (readKeysInMigration.isEmpty() && writeKeysInMigration.isEmpty())
			return false;
		// 3) keys in Migration range have part migrated
		return someKeyMigrated;
	}

	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}

	/**
	 * This method will be called by execute(). The default implementation of
	 * this method follows the steps described by Calvin paper.
	 */
	protected void executeTransactionLogic() {

		// Read the local records
		// Map<RecordKey, CachedRecord>
		readings = new HashMap<RecordKey, CachedRecord>();

		// For pulling
		if (activePulling) {
			if (isSourceNode)
				pushMigrationData();
			else if (isDestNode)
				pullMigrationData(readings);
		}

		// if (isInMigrating && !isExecutingInSrc && isSourceNode)
		// return;

		// Read the local records
		performLocalRead(readings);

		// Push local records to the needed remote nodes
		pushReadingsToRemotes(readings);

		// Passive participants stops here
		if (isPassiveParticipant)
			return;

		// Read the remote records
		collectRemoteReadings(readings);

		if (islog && (this.isSourceNode || this.isDestNode)) {
			String str = "********\n Txnum : " + txNum;
			str = str + "\n Final Readings : ";
			for (Integer k : recordKeyToSortArray(readings.keySet()))
				str = str + " , " + k;
			str = str + "\n********";
			System.out.println(str);
		}

		// Write the local records
		executeSql(readings);
	}

	protected void addReadKey(RecordKey readKey) {

		// Check if it is a fully replicated key
		if (Elasql.partitionMetaMgr().isFullyReplicated(readKey)) {
			fullyRepKeys.add(readKey);
			return;
		}
		int nodeId = Elasql.partitionMetaMgr().getPartition(readKey);

		// Check which node has the corresponding record
		// Normal
		// if (!isInMigrating || !migraMgr.keyIsInMigrationRange(readKey)) {
		//
		// if (nodeId == localNodeId)
		// localReadKeys.add(readKey);
		// else
		// remoteReadKeys.add(readKey);
		// } else { // Migrating
		//
		// // Check the migration status
		// if (!migraMgr.isRecordMigrated(readKey)) {
		// pullKeys.add(readKey);
		// } else
		// isExecutingInSrc = false;
		//
		// readKeysInMigration.add(readKey);
		// // Other nodes
		// if (!isSourceNode && !isDestNode)
		// remoteReadKeys.add(readKey);
		// }

		// All New design
		// Calvin part
		if (nodeId == localNodeId)
			localReadKeys.add(readKey);
		else
			remoteReadKeys.add(readKey);
		// Squall part
		if (isInMigrating) {
			if (migraMgr.keyIsInMigrationRange(readKey)) {
				// Record Read keys in migration range
				readKeysInMigration.add(readKey);
				if (!migraMgr.isRecordMigrated(readKey))
					pullKeys.add(readKey);
				else
					someKeyMigrated = true;
			}
		}

		// Record who is the node with most readings
		readsPerNodes[nodeId]++;
		if (readsPerNodes[nodeId] > readsPerNodes[mostReadsNode])
			mostReadsNode = nodeId;
	}

	protected void addWriteKey(RecordKey writeKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(writeKey);

		// // Normal
		// if (!isInMigrating || !migraMgr.keyIsInMigrationRange(writeKey)) {
		// if (nodeId == localNodeId)
		// localWriteKeys.add(writeKey);
		// activeParticipants.add(nodeId);
		// } else { // Migrating
		// // Check the migration status
		// if (!migraMgr.isRecordMigrated(writeKey)) {
		// pullKeys.add(writeKey);
		// } else {
		// isExecutingInSrc = false;
		// }
		//
		// writeKeysInMigration.add(writeKey);
		// }
		// All New design
		// Calvin part
		if (nodeId == localNodeId)
			localWriteKeys.add(writeKey);
		// Add activeParticipants
		activeParticipants.add(nodeId);

		// Squall part
		if (isInMigrating) {
			if (nodeId == migraMgr.getSourcePartition())
				SourceWriteKeys.add(writeKey);

			if (migraMgr.keyIsInMigrationRange(writeKey)) {
				// Record Write keys in migration range
				writeKeysInMigration.add(writeKey);
				// Check if record have migraed
				if (!migraMgr.isRecordMigrated(writeKey))
					pullKeys.add(writeKey);
				else
					someKeyMigrated = true;
			}
		}
	}

	protected void addInsertKey(RecordKey insertKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(insertKey);

		if ((isAnalyzing || isInMigrating) && migraMgr.keyIsInMigrationRange(insertKey)) {
			keysForBGPush.add(insertKey);
		}
		// Normal
		if (!isInMigrating || !migraMgr.keyIsInMigrationRange(insertKey)) {
			if (nodeId == localNodeId)
				localInsertKeys.add(insertKey);
			activeParticipants.add(nodeId);
		} else {// Migrating
			writeKeysInMigration.add(insertKey);
		}
	}

	protected void update(RecordKey key, CachedRecord rec) {
		if (localWriteKeys.contains(key))
			cacheMgr.update(key, rec);
	}

	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		if (localInsertKeys.contains(key))
			cacheMgr.insert(key, fldVals);
	}

	protected void delete(RecordKey key) {
		// XXX: Do we need a 'localDeleteKeys' for this ?
		if (localWriteKeys.contains(key))
			cacheMgr.delete(key);
	}

	protected void afterCommit() {

	}

	protected void waitForPullRequest() {
		CachedRecord rec = cacheMgr.readFromRemote(PULL_REQUEST_KEY);
		// CachedRecord rec = cacheMgr.read(PULL_REQUEST_KEY, txNum, tx, false);
		int value = (int) rec.getVal(DUMMY_FIELD1).asJavaVal();
		if (value != 0)
			throw new RuntimeException("something wrong for the pull request of tx." + txNum);
	}

	protected void sendAPullRequest(int nodeId) {
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put(DUMMY_FIELD1, new IntegerConstant(0));
		fldVals.put(DUMMY_FIELD2, new IntegerConstant(0));
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);

		TupleSet ts = new TupleSet(-4);
		ts.addTuple(PULL_REQUEST_KEY, txNum, txNum, rec);
		Elasql.connectionMgr().pushTupleSet(nodeId, ts);
	}

	private void performLocalRead(Map<RecordKey, CachedRecord> localReadings) {

		// Read local records (for both active or passive participants)
		for (RecordKey k : localReadKeys) {
			CachedRecord rec = cacheMgr.readFromLocal(k);
			localReadings.put(k, rec);
		}

		// Read the fully replicated records (for only active participants)
		if (isActiveParticipant) {
			for (RecordKey k : fullyRepKeys) {
				CachedRecord rec = cacheMgr.readFromLocal(k);
				localReadings.put(k, rec);
			}
		}

	}

	private void pushReadingsToRemotes(Map<RecordKey, CachedRecord> readings) {
		// If there is only one active participant, and you are that one,
		// return immediately.

		if (activeParticipants.size() < 2 && isActiveParticipant)
			return;

		TupleSet ts = new TupleSet(-3);
		if (!readings.isEmpty()) {
			// Construct pushing tuple set
			for (Entry<RecordKey, CachedRecord> e : readings.entrySet()) {
				if (!fullyRepKeys.contains(e.getKey()))
					ts.addTuple(e.getKey(), txNum, txNum, e.getValue());
			}

			// Push to all active participants
			for (Integer n : activeParticipants)
				if (n != localNodeId)
					Elasql.connectionMgr().pushTupleSet(n, ts);
		}
	}

	private void collectRemoteReadings(Map<RecordKey, CachedRecord> readingCache) {
		// Read remote records
		for (RecordKey k : remoteReadKeys) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			readingCache.put(k, rec);
		}
	}

	// Only for the source node
	private void pushMigrationData() {

		// System.out.println("Push data at Source");
		// Wait for pull request
		waitForPullRequest();

		// Perform reading for pulling data
		TupleSet ts = new TupleSet(-1);
		for (RecordKey k : pullKeys) {
			CachedRecord rec = cacheMgr.readFromLocal(k);
			if (rec == null)
				throw new RuntimeException(
						"Tx. " + txNum + " cannot find the record for " + k + " in the local storage.");
			ts.addTuple(k, txNum, txNum, rec);
		}

		// Push migration data set
		Elasql.connectionMgr().pushTupleSet(migraMgr.getDestPartition(), ts);
	}

	// Only for the destination node
	private void pullMigrationData(Map<RecordKey, CachedRecord> readings) {
		// System.out.println("Pull data at Destination");
		// Send pull request
		sendAPullRequest(migraMgr.getSourcePartition());

		// Receiving migration data set
		for (RecordKey k : pullKeys) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			// destination should do insert rather than write
			cacheMgr.setInsert(k, rec);
			readings.put(k, rec);
		}
	}

	private ArrayList<Integer> recordKeyToSortArray(Set<RecordKey> s) {
		ArrayList<Integer> l = new ArrayList<Integer>();
		for (RecordKey k : s)
			l.add((int) k.getKeyVal("i_id").asJavaVal());
		Collections.sort(l);
		return l;
	}

	public static void main(String[] a) {

		Set<RecordKey> set = new HashSet<RecordKey>();
		HashMap<String, Constant> tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(1));
		RecordKey tmpK = new RecordKey("item", tmp);
		set.add(tmpK);
		tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(2));
		tmpK = new RecordKey("item", tmp);
		set.add(tmpK);
		tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(3));
		tmpK = new RecordKey("item", tmp);
		set.add(tmpK);
		tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(4));
		tmpK = new RecordKey("item", tmp);
		set.add(tmpK);

		Set<RecordKey> set2 = new HashSet<RecordKey>();
		tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(3));
		tmpK = new RecordKey("item", tmp);
		set2.add(tmpK);
		tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(4));
		tmpK = new RecordKey("item", tmp);
		set2.add(tmpK);
		tmp = new HashMap<String, Constant>();
		tmp.put("i_id", new IntegerConstant(5));
		tmpK = new RecordKey("item", tmp);
		set2.add(tmpK);

		set.removeAll(set2);
		System.out.println(set);
	}

}
