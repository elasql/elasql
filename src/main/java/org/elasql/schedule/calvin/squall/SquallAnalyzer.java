package org.elasql.schedule.calvin.squall;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.migration.squall.SquallMigrationMgr;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;

public class SquallAnalyzer implements ReadWriteSetAnalyzer {
	
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
		
		@Override
		public String toString() {
			return String.format("[Part.%d -> Part.%d]", sourceId, destId);
		}
	}
	
	private static Set<PrimaryKey> getKeySet(Map<MigrationPath, Set<PrimaryKey>> map,
			MigrationPath path) {
		Set<PrimaryKey> keys = map.get(path);
		if (keys == null) {
			keys = new HashSet<PrimaryKey>();
			map.put(path, keys);
		}
		return keys;
	}
	
	private int localNodeId = Elasql.serverId();
	private ExecutionPlan execPlan;
	private SquallMigrationMgr migraMgr;
	
	// For read-only transactions to choose one node as a active participant
	private int[] readsPerNodes;
	
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private Set<PrimaryKey> fullyRepReadKeys = new HashSet<PrimaryKey>();
	
	// Cache the keys until we decide which nodes handle them
	private Map<MigrationPath, Set<PrimaryKey>> migratingReadKeys
		= new HashMap<MigrationPath, Set<PrimaryKey>>();
	private Map<MigrationPath, Set<PrimaryKey>> migratingUpdateKeys
		= new HashMap<MigrationPath, Set<PrimaryKey>>();
	private Map<MigrationPath, Set<PrimaryKey>> migratingInsertKeys
		= new HashMap<MigrationPath, Set<PrimaryKey>>();
	private Map<MigrationPath, Set<PrimaryKey>> migratingDeleteKeys
		= new HashMap<MigrationPath, Set<PrimaryKey>>();
	private Map<MigrationPath, Boolean> executeOnDest
		= new HashMap<MigrationPath, Boolean>();
	private Map<PrimaryKey, Boolean> migratingRecords = new HashMap<PrimaryKey, Boolean>();
	
	public SquallAnalyzer() {
		execPlan = new ExecutionPlan();
		readsPerNodes = new int[Elasql.partitionMetaMgr().getCurrentNumOfParts()];
		migraMgr = (SquallMigrationMgr) Elasql.migrationMgr();
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
	public void addReadKey(PrimaryKey readKey) {
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
				
				if (migraMgr.isMigrated(readKey)) {
					executeOnDest.put(path, Boolean.TRUE);
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
	public void addUpdateKey(PrimaryKey updateKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(updateKey)) {
			execPlan.addLocalUpdateKey(updateKey);
		} else {
			if (migraMgr.isMigratingRecord(updateKey)) {
				int sourceNode = migraMgr.checkSourceNode(updateKey);
				int destNode = migraMgr.checkDestNode(updateKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingUpdateKeys, path).add(updateKey);
				
				if (migraMgr.isMigrated(updateKey)) {
					executeOnDest.put(path, Boolean.TRUE);
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
	public void addInsertKey(PrimaryKey insertKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(insertKey)) {
			execPlan.addLocalInsertKey(insertKey);
		} else {
			if (migraMgr.isMigratingRecord(insertKey)) {
				int sourceNode = migraMgr.checkSourceNode(insertKey);
				int destNode = migraMgr.checkDestNode(insertKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingInsertKeys, path).add(insertKey);
				executeOnDest.putIfAbsent(path, Boolean.FALSE);
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(insertKey);
				if (nodeId == localNodeId)
					execPlan.addLocalInsertKey(insertKey);
				
				activeParticipants.add(nodeId);
			}
		}
	}
	
	@Override
	public void addDeleteKey(PrimaryKey deleteKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(deleteKey)) {
			execPlan.addLocalDeleteKey(deleteKey);
		} else {
			if (migraMgr.isMigratingRecord(deleteKey)) {
				int sourceNode = migraMgr.checkSourceNode(deleteKey);
				int destNode = migraMgr.checkDestNode(deleteKey);
				MigrationPath path = new MigrationPath(sourceNode, destNode);
				
				// We cache the migrating keys and handle them later.
				getKeySet(migratingDeleteKeys, path).add(deleteKey);
				executeOnDest.putIfAbsent(path, Boolean.FALSE);
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(deleteKey);
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
				Set<PrimaryKey> keys = migratingReadKeys.get(p);
				if (keys != null)
					addToForegourndMigration(p, keys);
				keys = migratingUpdateKeys.get(p);
				if (keys != null)
					addToForegourndMigration(p, keys);
			} else {
				putMigratingKeysTo(p, p.sourceId);
				
				// New inserted data should be also migrated to the destination node
				Set<PrimaryKey> insertKeys = migratingInsertKeys.get(p);
				if (insertKeys != null)
					for (PrimaryKey key : insertKeys)
						migraMgr.addNewInsertKeyOnSource(key);
			}
		}
	}
	
	private void putMigratingKeysTo(MigrationPath p, int executionNodeId) {
		if (localNodeId == executionNodeId) {
			if (migratingReadKeys.get(p) != null)
				for (PrimaryKey k : migratingReadKeys.get(p))
					execPlan.addLocalReadKey(k);
			if (migratingUpdateKeys.get(p) != null)
				for (PrimaryKey k : migratingUpdateKeys.get(p))
					execPlan.addLocalUpdateKey(k);
			if (migratingInsertKeys.get(p) != null)
				for (PrimaryKey k : migratingInsertKeys.get(p))
					execPlan.addLocalInsertKey(k);
			if (migratingDeleteKeys.get(p) != null)
				for (PrimaryKey k : migratingDeleteKeys.get(p))
					execPlan.addLocalDeleteKey(k);
		} else {
			if (migratingReadKeys.get(p) != null)
				for (PrimaryKey k : migratingReadKeys.get(p))
					execPlan.addRemoteReadKey(k);
		}
		if (migratingReadKeys.get(p) != null)
			readsPerNodes[executionNodeId] += migratingReadKeys.get(p).size();
		if (migratingUpdateKeys.get(p) != null || migratingInsertKeys.get(p) != null ||
				migratingDeleteKeys.get(p) != null)
			activeParticipants.add(executionNodeId);
	}

	private void addToForegourndMigration(MigrationPath p, Set<PrimaryKey> keySet) {
		for (PrimaryKey k : keySet) {
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
				for (PrimaryKey key : execPlan.getLocalReadKeys())
					execPlan.addPushSet(target, key);
			}
		}
	}
	
	private void activePartReadFullyReps() {
		for (PrimaryKey key : fullyRepReadKeys)
			execPlan.addLocalReadKey(key);
	}
	
	private void updateMigrationStatus() {
		for (PrimaryKey key : migratingRecords.keySet())
			if (migratingRecords.get(key))
				migraMgr.setMigrated(key);
	}
}
