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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.cache.calvin.CalvinRemotePostOffice;
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
	protected CalvinCacheMgr cacheMgr;

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
	// XXX: Do we need table-level locks ?
	private Set<RecordKey> localReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localWriteKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localInsertKeys = new HashSet<RecordKey>();
	private Set<RecordKey> remoteReadKeys = new HashSet<RecordKey>();

	// Records
	private Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;
		this.localNodeId = Elasql.serverId();

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
		tx = Elasql.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		tx.addLifecycleListener(new DdRecoveryMgr(tx.getTransactionNumber()));

		// prepare keys
		prepareKeys();

		// decide which node the master is
		masterNode = decideMaster();
		
		// for the cache layer
		CalvinRemotePostOffice postOffice = (CalvinRemotePostOffice) Elasql.remoteRecReceiver();
		if (isParticipated()) {
			// create a cache manager
			if (remoteReadKeys.isEmpty())
				cacheMgr = postOffice.createCacheMgr(tx, false);
			else
				cacheMgr = postOffice.createCacheMgr(tx, true);
		} else {
			postOffice.skipTransaction(txNum);
		}
	}

	public void bookConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		
		ccMgr.bookReadKeys(localReadKeys);
		ccMgr.bookWriteKeys(localWriteKeys);
		ccMgr.bookWriteKeys(localInsertKeys);
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

		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			paramHelper.setCommitted(false);
		} finally {
			// Clean the cache
			cacheMgr.notifyTxCommitted();
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
		return localReadKeys.size() != 0 || localWriteKeys.size() != 0 ||
				localInsertKeys.size() != 0;
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
	}

	protected void addReadKey(RecordKey readKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(readKey);
		
		readPerNode[nodeId]++;

		if (nodeId == localNodeId)
			localReadKeys.add(readKey);
		else
			remoteReadKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(writeKey);

		writePerNode[nodeId]++;

		if (nodeId == localNodeId)
			localWriteKeys.add(writeKey);
		activeParticipants.add(nodeId);
	}
	
	protected void addInsertKey(RecordKey insertKey) {
		// Check which node has the corresponding record
		int nodeId = Elasql.partitionMetaMgr().getPartition(insertKey);

		writePerNode[nodeId]++;

		if (nodeId == localNodeId)
			localInsertKeys.add(insertKey);
		activeParticipants.add(nodeId);
	}

	protected Map<RecordKey, CachedRecord> getReadings() {
		return readings;
	}
	
	protected void update(RecordKey key, CachedRecord rec) {
		if (localWriteKeys.contains(key))
			cacheMgr.update(key, rec);
	}
	
	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		if (localInsertKeys.contains(key))
			cacheMgr.insert(key, fldVals);
	}

	private void performLocalRead() {
		Map<RecordKey, CachedRecord> localReadings = new HashMap<RecordKey, CachedRecord>();
		// Check which node has the corresponding record
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();

		// Read local records
		for (RecordKey k : localReadKeys) {
			if (!partMgr.isFullyReplicated(k) || activeParticipants.contains(localNodeId)) {
				CachedRecord rec = cacheMgr.readFromLocal(k);
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
			CachedRecord rec = cacheMgr.readFromRemote(k);
			readings.put(k, rec);
			remoteReadings.put(k, rec);
		}

		// Notify the implementation of subclass
		onRemoteReadCollected(remoteReadings);
	}
}
