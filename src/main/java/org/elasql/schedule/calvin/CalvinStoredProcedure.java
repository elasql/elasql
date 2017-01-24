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
package org.elasql.schedule.calvin;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.DdStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper>
		implements DdStoredProcedure {

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;
	protected int localNodeId = Elasql.serverId();

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	// private Set<Integer> passiveParticipants = new HashSet<Integer>();

	// Master node decision
	private int[] readPerNode = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int[] writePerNode = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int masterNode;

	// Record keys
	private List<String> localReadTables = new ArrayList<String>();
	private List<String> localWriteTables = new ArrayList<String>();
	private List<RecordKey> localReadKeys = new ArrayList<RecordKey>();
	private List<RecordKey> localWriteKeys = new ArrayList<RecordKey>();
	private List<RecordKey> remoteReadKeys = new ArrayList<RecordKey>();

	// Records
	private Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();

	private ConservativeOrderedCcMgr ccMgr;
	private String[] readTablesForLock, writeTablesForLock;
	private RecordKey[] readKeysForLock, writeKeysForLock;
	
	private CalvinCacheMgr cacheMgr = (CalvinCacheMgr) Elasql.cacheMgr();

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
	protected abstract void prepareKeys();

	protected abstract void onLocalReadCollected(
			Map<RecordKey, CachedRecord> localReadings);

	protected abstract void onRemoteReadCollected(
			Map<RecordKey, CachedRecord> remoteReadings);

	protected abstract void writeRecords(
			Map<RecordKey, CachedRecord> remoteReadings);

	protected abstract void masterCollectResults(
			Map<RecordKey, CachedRecord> readings);

	/**********************
	 * implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// create transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		this.tx = Elasql.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));

		// prepare keys
		prepareKeys();

		// decide which node the master is
		masterNode = decideMaster();
	}

	public void requestConservativeLocks() {
		readTablesForLock = localReadTables.toArray(new String[0]);
		writeTablesForLock = localWriteTables.toArray(new String[0]);

		readKeysForLock = localReadKeys.toArray(new RecordKey[0]);
		writeKeysForLock = localWriteKeys.toArray(new RecordKey[0]);

		ccMgr.prepareSp(readTablesForLock, writeTablesForLock);
		ccMgr.prepareSp(readKeysForLock, writeKeysForLock);
	}

	private void getConservativeLocks() {
		ccMgr.executeSp(readTablesForLock, writeTablesForLock);
		ccMgr.executeSp(readKeysForLock, writeKeysForLock);
	}

	@Override
	public final RecordKey[] getReadSet() {
		return localReadKeys.toArray(new RecordKey[0]);
	}

	@Override
	public final RecordKey[] getWriteSet() {
		return localWriteKeys.toArray(new RecordKey[0]);
	}

	@Override
	public SpResultSet execute() {
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();

			// Execute transaction
			executeTransactionLogic();

			// The transaction finishes normally
			tx.commit();

		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			paramHelper.setCommitted(false);
		}

		return paramHelper.createResultSet();
	}

	/**
	 * Returns true if this machine is the master node which is responsible for
	 * sending response back to client.
	 * 
	 * @return
	 */
	public boolean isMasterNode() {
		return masterNode == localNodeId;
	}

	public int getMasterNodeId() {
		return masterNode;
	}

	public boolean isParticipated() {
		if (masterNode == localNodeId)
			return true;
		return localReadTables.size() != 0 || localWriteTables.size() != 0
				|| localReadKeys.size() != 0 || localWriteKeys.size() != 0;
	}

	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}

	/**
	 * Choose a node be the master node. The master node must collect the
	 * readings from other nodes and take responsibility for reporting to
	 * clients. This method can be overridden if a developer wants to use
	 * another election algorithm. The default algorithm chooses the node which
	 * has the most writings or readings to be a master node. It has to be
	 * noticed that the algorithm must be deterministic on all server nodes.
	 * 
	 * @return the master node id
	 */
	protected int decideMaster() {
		int maxValue = -1;
		int masterId = -1;

		// Let the node with the most writings be the master
		for (int nodeId = 0; nodeId < writePerNode.length; nodeId++) {
			if (maxValue < writePerNode[nodeId]) {
				maxValue = writePerNode[nodeId];
				masterId = nodeId;
			}
		}

		if (maxValue > 0)
			return masterId;

		// Let the node with the most readings be the master
		for (int nodeId = 0; nodeId < readPerNode.length; nodeId++) {
			if (maxValue < readPerNode[nodeId]) {
				maxValue = readPerNode[nodeId];
				masterId = nodeId;
			}
		}

		return masterId;
	}

	/**
	 * This method will be called by execute(). The default implementation of
	 * this method follows the steps described by Calvin paper.
	 */
	protected void executeTransactionLogic() {
		// Read the local records
		performLocalRead();

		// Push local records to the needed remote nodes
		if (!activeParticipants.isEmpty() || !isMasterNode())
			pushReadingsToRemotes();

		// Read the remote records
		if (activeParticipants.contains(localNodeId) || isMasterNode())
			collectRemoteReadings();

		// Write the local records
		if (activeParticipants.contains(localNodeId))
			writeRecords(readings);

		// The master node must collect final results
		if (isMasterNode())
			masterCollectResults(readings);

		// Remove the cached records in CacheMgr
		removeCachedRecord();
	}

	protected void addReadKey(RecordKey readKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(readKey);
		int localNodeId = Elasql.serverId();
		
		try {
			readPerNode[nodeId]++;
		} catch (Exception e) {
			System.out.println(readKey);
			throw e;
		}

		if (nodeId == localNodeId)
			localReadKeys.add(readKey);
		else
			remoteReadKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(writeKey);
		int localNodeId = Elasql.serverId();

		writePerNode[nodeId]++;

		if (nodeId == localNodeId)
			localWriteKeys.add(writeKey);
		activeParticipants.add(nodeId);
	}

	protected Map<RecordKey, CachedRecord> getReadings() {
		return readings;
	}

	protected List<RecordKey> getLocalWriteKeys() {
		return localWriteKeys;
	}
	
	protected void update(RecordKey key, CachedRecord rec) {
		if (localWriteKeys.contains(key))
			cacheMgr.update(key, rec, tx);
	}
	
	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		if (localWriteKeys.contains(key))
			cacheMgr.insert(key, fldVals, tx);
	}

	private void performLocalRead() {
		Map<RecordKey, CachedRecord> localReadings = new HashMap<RecordKey, CachedRecord>();
		// Check which node has the corresponding record
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();

		// Read local records
		for (RecordKey k : localReadKeys) {
			if (!partMgr.isFullyReplicated(k) || activeParticipants.contains(localNodeId)) {
				CachedRecord rec = cacheMgr.read(k, txNum, tx, true);
				readings.put(k, rec);
				localReadings.put(k, rec);
			}
		}

		// Notify the implementation of subclass
		onLocalReadCollected(localReadings);
	}

	private void pushReadingsToRemotes() {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		TupleSet ts = new TupleSet(-1);
		if (!readings.isEmpty()) {
			// Construct pushing tuple set
			for (Entry<RecordKey, CachedRecord> e : readings.entrySet()) {
				if (!partMgr.isFullyReplicated(e.getKey()))
					ts.addTuple(e.getKey(), txNum, txNum, e.getValue());
			}

			// Push to all active participants
			for (Integer n : activeParticipants)
				if (n != localNodeId)
					Elasql.connectionMgr().pushTupleSet(n, ts);

			// Push a set to the master node
			if (!activeParticipants.contains(masterNode) && !isMasterNode())
				Elasql.connectionMgr().pushTupleSet(masterNode, ts);
		}
	}

	private void collectRemoteReadings() {
		Map<RecordKey, CachedRecord> remoteReadings = new HashMap<RecordKey, CachedRecord>();

		// Read remote records
		for (RecordKey k : remoteReadKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, false);
			readings.put(k, rec);
			remoteReadings.put(k, rec);
		}

		// Notify the implementation of subclass
		onRemoteReadCollected(remoteReadings);
	}

	private void removeCachedRecord() {
		// write to local storage and remove cached records
		for (RecordKey k : localWriteKeys) {
			cacheMgr.flushToLocalStorage(k, txNum, tx);
			cacheMgr.remove(k, txNum);
		}

		// remove cached read records
		for (RecordKey k : localReadKeys)
			cacheMgr.remove(k, txNum);
		for (RecordKey k : remoteReadKeys)
			cacheMgr.remove(k, txNum);
	}
}
