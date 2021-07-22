package org.elasql.schedule.calvin.mgcrab;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationMgr;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;

public class CaughtUpAnalyzer implements ReadWriteSetAnalyzer {
	
	private int localNodeId = Elasql.serverId();
	private ExecutionPlan execPlan;
	private MigrationMgr migraMgr;
	
	// For read-only transactions to choose one node as a active participant
	private int[] readsPerNodes;
	
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private Set<PrimaryKey> fullyRepReadKeys = new HashSet<PrimaryKey>();
	
	// To update the migrating records in the end for all the nodes
	private Set<PrimaryKey> migratingRecords = new HashSet<PrimaryKey>();
	
	public CaughtUpAnalyzer() {
		execPlan = new ExecutionPlan();
		readsPerNodes = new int[Elasql.partitionMetaMgr().getCurrentNumOfParts()];
		migraMgr = Elasql.migrationMgr();
	}
	
	@Override
	public ExecutionPlan generatePlan() {
		
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

				// For foreground migration
				if (!migraMgr.isMigrated(readKey)) {
					if (localNodeId == sourceNode) {
						execPlan.addReadsForMigration(readKey);
						execPlan.addMigrationPushSet(destNode, readKey);
					} else if (localNodeId == destNode) {
						execPlan.addImcomingMigratingKeys(readKey);
					}
				
					migratingRecords.add(readKey);
				}
				
				// For normal operations
				if (localNodeId == sourceNode) {
					if (migraMgr.isMigrated(readKey)) {
						execPlan.addRemoteReadKey(readKey);
					} else {
						execPlan.addLocalReadKey(readKey);
					}
				} else if (localNodeId == destNode) {
					execPlan.addLocalReadKey(readKey);
				} else {
					execPlan.addRemoteReadKey(readKey);
				}
				
				readsPerNodes[destNode]++;
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
				
				// For foreground migration
				if (!migraMgr.isMigrated(updateKey)) {
					if (localNodeId == sourceNode) {
						execPlan.addReadsForMigration(updateKey);
						execPlan.addMigrationPushSet(destNode, updateKey);
					} else if (localNodeId == destNode) {
						execPlan.addImcomingMigratingKeys(updateKey);
					}
				
					migratingRecords.add(updateKey);
				}
				
				// For normal operations
				if (localNodeId == destNode)
					execPlan.addLocalUpdateKey(updateKey);
				
				activeParticipants.add(destNode);
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
				int destNode = migraMgr.checkDestNode(insertKey);
				
				if (localNodeId == destNode)
					execPlan.addLocalInsertKey(insertKey);
				
				activeParticipants.add(destNode);
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
				int destNode = migraMgr.checkDestNode(deleteKey);
				
				if (localNodeId == destNode)
					execPlan.addLocalDeleteKey(deleteKey);
				
				activeParticipants.add(destNode);
			} else {
				int nodeId = Elasql.partitionMetaMgr().getPartition(deleteKey);
				if (nodeId == localNodeId)
					execPlan.addLocalDeleteKey(deleteKey);
				
				activeParticipants.add(nodeId);
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
		for (PrimaryKey key : migratingRecords)
			migraMgr.setMigrated(key);
	}
}
