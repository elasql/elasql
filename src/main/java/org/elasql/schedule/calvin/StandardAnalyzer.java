package org.elasql.schedule.calvin;

import java.util.HashSet;
import java.util.Set;

import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;

public class StandardAnalyzer implements ReadWriteSetAnalyzer {
	
	private int localNodeId = Elasql.serverId();
	private ExecutionPlan execPlan;
	
	// For read-only transactions to choose one node as a active participant
	private int mostReadsNode;
	private int[] readsPerNodes;
	
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private Set<PrimaryKey> fullyRepReadKeys = new HashSet<PrimaryKey>();
	
	public StandardAnalyzer() {
		execPlan = new ExecutionPlan();
		mostReadsNode = 0;
		readsPerNodes = new int[Elasql.partitionMetaMgr().getCurrentNumOfParts()];
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
		
		return execPlan;
	}

	@Override
	public void addReadKey(PrimaryKey readKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(readKey)) {
			// We cache it then check if we should add it to the local read set later
			fullyRepReadKeys.add(readKey);
		} else {
			int nodeId = Elasql.partitionMetaMgr().getPartition(readKey);
			if (nodeId == localNodeId)
				execPlan.addLocalReadKey(readKey);
			else
				execPlan.addRemoteReadKey(readKey);
			
			// Record who is the node with most readings
			readsPerNodes[nodeId]++;
			if (readsPerNodes[nodeId] > readsPerNodes[mostReadsNode])
				mostReadsNode = nodeId;
		}
	}
	
	@Override
	public void addUpdateKey(PrimaryKey updateKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(updateKey)) {
			execPlan.addLocalUpdateKey(updateKey);
		} else {
			int nodeId = Elasql.partitionMetaMgr().getPartition(updateKey);
			if (nodeId == localNodeId)
				execPlan.addLocalUpdateKey(updateKey);
			activeParticipants.add(nodeId);
		}
	}
	
	@Override
	public void addInsertKey(PrimaryKey insertKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(insertKey)) {
			execPlan.addLocalInsertKey(insertKey);
		} else {
			int nodeId = Elasql.partitionMetaMgr().getPartition(insertKey);
			if (nodeId == localNodeId)
				execPlan.addLocalInsertKey(insertKey);
			activeParticipants.add(nodeId);
		}
	}
	
	@Override
	public void addDeleteKey(PrimaryKey deleteKey) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(deleteKey)) {
			execPlan.addLocalDeleteKey(deleteKey);
		} else {
			int nodeId = Elasql.partitionMetaMgr().getPartition(deleteKey);
			if (nodeId == localNodeId)
				execPlan.addLocalDeleteKey(deleteKey);
			activeParticipants.add(nodeId);
		}
	}

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private void decideRole() {
		// if there is no active participant (e.g. read-only transaction),
		// choose the one with most readings as the only active participant.
		if (activeParticipants.isEmpty())
			activeParticipants.add(mostReadsNode);
		
		// Decide the role
		if (activeParticipants.contains(localNodeId)) {
			execPlan.setParticipantRole(ParticipantRole.ACTIVE);
		} else if (execPlan.hasLocalReads()) {
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
}
