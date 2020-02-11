package org.elasql.schedule.calvin.Zephyr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.migration.zephyr.ZephyrMigrationMgr;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class ZephyrAnalyzer implements ReadWriteSetAnalyzer {
	
	private static class MigrationPath {
		int sourceId, destId;
		
		public MigrationPath(int sourceId, int destId) {
			this.sourceId = sourceId;
			this.destId = destId;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			
			if (!obj.getClass().equals(this.getClass()))
				return false;
			
			MigrationPath p = (MigrationPath) obj;
			return this.sourceId == p.sourceId &&
					this.destId == p.destId;
		}
		
		@Override
		public int hashCode() {
			int hash = 7;
		    hash = 31 * hash + sourceId;
		    hash = 31 * hash + destId;
		    return hash;
		}
	}
	
	private static Set<RecordKey> getKeySet(Map<MigrationPath, Set<RecordKey>> map,
			MigrationPath path) {
		Set<RecordKey> keys = map.get(path);
		if (keys == null) {
			keys = new HashSet<RecordKey>();
			map.put(path, keys);
		}
		return keys;
	}
	
	private int localNodeId = Elasql.serverId();
	private ExecutionPlan execPlan;
	private ZephyrMigrationMgr migraMgr;
	
	// For read-only transactions to choose one node as a active participant
	private int[] readsPerNodes;
	
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private Set<RecordKey> fullyRepReadKeys = new HashSet<RecordKey>();
	
	// Cache the keys until we decide which nodes handle them
	private Map<MigrationPath, Set<RecordKey>> migratingReadKeys
		= new HashMap<MigrationPath, Set<RecordKey>>();
	private Map<MigrationPath, Set<RecordKey>> migratingUpdateKeys
		= new HashMap<MigrationPath, Set<RecordKey>>();
	private Map<MigrationPath, Set<RecordKey>> migratingInsertKeys
		= new HashMap<MigrationPath, Set<RecordKey>>();
	private Map<MigrationPath, Set<RecordKey>> migratingDeleteKeys
		= new HashMap<MigrationPath, Set<RecordKey>>();
	private Map<MigrationPath, Boolean> executeOnDest
		= new HashMap<MigrationPath, Boolean>();
	private Map<RecordKey, Boolean> migratingRecords = new HashMap<RecordKey, Boolean>();
	
	public ZephyrAnalyzer() {
		execPlan = new ExecutionPlan();
		readsPerNodes = new int[Elasql.partitionMetaMgr().getCurrentNumOfParts()];
		migraMgr = (ZephyrMigrationMgr) Elasql.migrationMgr();
	}
	
	@Override
	public ExecutionPlan generatePlan() {
		
		decideExecutionNodeForMigration();
		
		decideRole();
		
		if (execPlan.getParticipantRole() != ParticipantRole.IGNORE) {
			generatePushSets();
		}
		
		if (execPlan.getParticipantRole() == ParticipantRole.ACTIVE) {
			activePartReadFullyReps();
		}
		
		updateMigrationStatus();
		
		return execPlan;
	}

	@Override
	public void addReadKey(RecordKey readKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(readKey)) {
			// We cache it then check if we should add it to the local read set later
			fullyRepReadKeys.add(readKey);
		} else {
			if (migraMgr.isMigratingRecord(readKey)) {
				int sourceNode = migraMgr.checkSourceNode(readKey);
				int destNode = migraMgr.checkDestNode(readKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingReadKeys, path).add(readKey);
				if (migraMgr.isInDual()) {
					executeOnDest.put(path, Boolean.TRUE);
					if(!migraMgr.isMigrated(readKey)) {
						migratingRecords.putIfAbsent(readKey, Boolean.FALSE);
					}	
				} else {
					migratingRecords.putIfAbsent(readKey, Boolean.FALSE);
					executeOnDest.putIfAbsent(path, Boolean.FALSE);
				}
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(readKey);
				if (nodeId == localNodeId)
					execPlan.addLocalReadKey(readKey);
				else
					execPlan.addRemoteReadKey(readKey);
				
				// Record who is the node with most readings
				readsPerNodes[nodeId]++;
			}
		}
	}
	
	@Override
	public void addUpdateKey(RecordKey updateKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(updateKey)) {
			execPlan.addLocalUpdateKey(updateKey);
		} else {
			//execPlan.setNeedAbort();
			if (migraMgr.isMigratingRecord(updateKey)) {
				int sourceNode = migraMgr.checkSourceNode(updateKey);
				int destNode = migraMgr.checkDestNode(updateKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingUpdateKeys, path).add(updateKey);
				
				if (migraMgr.isInDual()) {
					executeOnDest.put(path, Boolean.TRUE);
					if(!migraMgr.isMigrated(updateKey)) {
						migratingRecords.putIfAbsent(updateKey, Boolean.FALSE);
					}	
				} else {
					migratingRecords.putIfAbsent(updateKey, Boolean.FALSE);
					executeOnDest.putIfAbsent(path, Boolean.FALSE);
				}
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(updateKey);
				if (nodeId == localNodeId)
					execPlan.addLocalUpdateKey(updateKey);
				
				activeParticipants.add(nodeId);
			}
		}
	}
	
	@Override
	public void addInsertKey(RecordKey insertKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(insertKey)) {
			execPlan.addLocalInsertKey(insertKey);
		} else {
//			execPlan.setNeedAbort();
			if (migraMgr.isMigratingRecord(insertKey)) {
				execPlan.setNeedAbort();
				int sourceNode = migraMgr.checkSourceNode(insertKey);
				int destNode = migraMgr.checkDestNode(insertKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingInsertKeys, path).add(insertKey);
				executeOnDest.putIfAbsent(path, Boolean.FALSE);
//				execPlan.setNeedAbort();
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(insertKey);
				if((migraMgr.getSourceId().contains(nodeId) || migraMgr.getDestId().contains(nodeId)))
					execPlan.setNeedAbort();
				if (nodeId == localNodeId)
					execPlan.addLocalInsertKey(insertKey);
				
				activeParticipants.add(nodeId);
			}
		}
	}
	
	@Override
	public void addDeleteKey(RecordKey deleteKey) {	
		if (Elasql.partitionMetaMgr().isFullyReplicated(deleteKey)) {
			execPlan.addLocalDeleteKey(deleteKey);
		} else {
			if (migraMgr.isMigratingRecord(deleteKey)) {
				execPlan.setNeedAbort();
				
				int sourceNode = migraMgr.checkSourceNode(deleteKey);
				int destNode = migraMgr.checkDestNode(deleteKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingDeleteKeys, path).add(deleteKey);
				executeOnDest.putIfAbsent(path, Boolean.FALSE);
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(deleteKey);
//				if(migraMgr.isInMigration() && nodeId == migraMgr.getSourceId())
//					execPlan.setNeedAbort();
				if (nodeId == localNodeId)
					execPlan.addLocalDeleteKey(deleteKey);
				
				activeParticipants.add(nodeId);
			}
		}
	}
	
	private void decideExecutionNodeForMigration() {
		for (MigrationPath p : executeOnDest.keySet()) {
			boolean isExecutingOnDest = executeOnDest.get(p);
			
			if (isExecutingOnDest) {
				putMigratingKeysTo(p, p.destId);
				
				// Add foreground migrations
				addToForegourndMigration(p, migratingReadKeys.get(p));
				addToForegourndMigration(p, migratingUpdateKeys.get(p));
			} else {
				putMigratingKeysTo(p, p.sourceId);
				
				// New inserted data should be also migrated to the destination node
				Set<RecordKey> insertKeys = migratingInsertKeys.get(p);
				if (insertKeys != null && !execPlan.isNeedAbort()) {
//					if(migraMgr.isInMigration())
//						execPlan.setNeedAbort();
					for (RecordKey key : insertKeys)
						migraMgr.addNewInsertKeyOnSource(key);
				}
			}
		}
	}
	
	private void putMigratingKeysTo(MigrationPath p, int executionNodeId) {
		if (localNodeId == executionNodeId) {
			if (migratingReadKeys.get(p) != null)
				for (RecordKey k : migratingReadKeys.get(p))
					execPlan.addLocalReadKey(k);
			if (migratingUpdateKeys.get(p) != null)
				for (RecordKey k : migratingUpdateKeys.get(p))
					execPlan.addLocalUpdateKey(k);
			if (migratingInsertKeys.get(p) != null)
				for (RecordKey k : migratingInsertKeys.get(p))
					execPlan.addLocalInsertKey(k);
			if (migratingDeleteKeys.get(p) != null)
				for (RecordKey k : migratingDeleteKeys.get(p))
					execPlan.addLocalDeleteKey(k);
		} else {
			if (migratingReadKeys.get(p) != null)
				for (RecordKey k : migratingReadKeys.get(p))
					execPlan.addRemoteReadKey(k);
		}
		if (migratingReadKeys.get(p) != null)
			readsPerNodes[executionNodeId] += migratingReadKeys.get(p).size();
		if (migratingUpdateKeys.get(p) != null || migratingInsertKeys.get(p) != null ||
				migratingDeleteKeys.get(p) != null)
			activeParticipants.add(executionNodeId);
	}

	private void addToForegourndMigration(MigrationPath p, Set<RecordKey> keySet) {
		for (RecordKey k : keySet) {
			if (migratingRecords.containsKey(k)) {
				migratingRecords.put(k, Boolean.TRUE);
				if (localNodeId == p.sourceId) {
					execPlan.addReadsForMigration(k);
					execPlan.addMigrationPushSet(p.destId, k);
				} else if (localNodeId == p.destId) {
					execPlan.addImcomingMigratingKeys(k);
					execPlan.addPullingSource(p.sourceId);
				}
			}
		}
	}
	

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private void decideRole() {
		// if there is no active participant (e.g. read-only transaction),
		// choose the one with most readings as the only active participant.
		if (activeParticipants.isEmpty()) {
			int mostReads = 0;
			for (int i = 0; i < readsPerNodes.length; i++) {
				if (readsPerNodes[i] > readsPerNodes[mostReads])
					mostReads = i;
			}
			activeParticipants.add(mostReads);
		}
		
		// Decide the role
		if (activeParticipants.contains(localNodeId)) {
			execPlan.setParticipantRole(ParticipantRole.ACTIVE);
		} else if (execPlan.hasLocalReads() || execPlan.hasMigrations()) {
			execPlan.setParticipantRole(ParticipantRole.PASSIVE);
		}
	}
	
	private void generatePushSets() {
		for (Integer target : activeParticipants) {
			if (target != localNodeId) {
				for (RecordKey key : execPlan.getLocalReadKeys())
					execPlan.addPushSet(target, key);
			}
		}
	}
	
	private void activePartReadFullyReps() {
		for (RecordKey key : fullyRepReadKeys)
			execPlan.addLocalReadKey(key);
	}
	
	private void updateMigrationStatus() {
		if(execPlan.isNeedAbort())
			return;
		for (RecordKey key : migratingRecords.keySet())
			if (migratingRecords.get(key))
				migraMgr.setMigrated(key);
	}
}
