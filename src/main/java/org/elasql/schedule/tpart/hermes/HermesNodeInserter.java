package org.elasql.schedule.tpart.hermes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class HermesNodeInserter implements BatchNodeInserter {
	
	private static final double IMBALANCED_TOLERANCE;

	static {
		IMBALANCED_TOLERANCE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(HermesNodeInserter.class.getName() + ".IMBALANCED_TOLERANCE", 0.25);
	}
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	private double[] loadPerPart = new double[PartitionMetaMgr.NUM_PARTITIONS];
	private Set<Integer> overloadedParts = new HashSet<Integer>();
	private Set<Integer> saturatedParts = new HashSet<Integer>();
	private int overloadedThreshold;

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		// Step 0: Reset statistics
		resetStatistics();
		
		// Step 1: Insert nodes to the graph
		for (TPartStoredProcedureTask task : tasks) {
			insertAccordingRemoteEdges(graph, task);
		}
		
		// Step 2: Find overloaded machines
		overloadedThreshold = (int) Math.ceil(
				((double) tasks.size() / partMgr.getCurrentNumOfParts()) * (IMBALANCED_TOLERANCE + 1));
		if (overloadedThreshold < 1) {
			overloadedThreshold = 1;
		}
		List<TxNode> candidateTxNodes = findTxNodesOnOverloadedParts(graph, tasks.size());
		
//		System.out.println(String.format("Overloaded threshold is %d (batch size: %d)", overloadedThreshold, tasks.size()));
//		System.out.println(String.format("Overloaded machines: %s, loads: %s", overloadedParts.toString(), Arrays.toString(loadPerPart)));
		
		// Step 3: Move tx nodes from overloaded machines to underloaded machines
		int increaseTolerence = 1;
		while (!overloadedParts.isEmpty()) {
//			System.out.println(String.format("Overloaded machines: %s, loads: %s, increaseTolerence: %d", overloadedParts.toString(), Arrays.toString(loadPerPart), increaseTolerence));
			candidateTxNodes = rerouteTxNodesToUnderloadedParts(candidateTxNodes, increaseTolerence);
			increaseTolerence++;
			
			if (increaseTolerence > 100)
				throw new RuntimeException("Something wrong");
		}
		
//		System.out.println(String.format("Final loads: %s", Arrays.toString(loadPerPart)));
	}
	
	private void resetStatistics() {
		Arrays.fill(loadPerPart, 0);
		overloadedParts.clear();
		saturatedParts.clear();
	}
	
	private void insertAccordingRemoteEdges(TGraph graph, TPartStoredProcedureTask task) {
		int bestPartId = 0;
		int minRemoteEdgeCount = task.getReadSet().size();
		
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			
			// Count the number of remote edge
			int remoteEdgeCount = countRemoteReadEdge(graph, task, partId);
			
			// Find the node in which the tx has fewest remote edges.
			if (remoteEdgeCount < minRemoteEdgeCount) {
				minRemoteEdgeCount = remoteEdgeCount;
				bestPartId = partId;
			}
		}
		
		graph.insertTxNode(task, bestPartId);
		
		loadPerPart[bestPartId]++;
	}
	
	private int countRemoteReadEdge(TGraph graph, TPartStoredProcedureTask task, int partId) {
		int remoteEdgeCount = 0;
		
		for (PrimaryKey key : task.getReadSet()) {
			// Skip replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			if (graph.getResourcePosition(key).getPartId() != partId) {
				remoteEdgeCount++;
			}
		}
		
		return remoteEdgeCount;
	}
	
	private List<TxNode> findTxNodesOnOverloadedParts(TGraph graph, int batchSize) {
		
		// Find the overloaded parts
		for (int partId = 0; partId < loadPerPart.length; partId++) {
			if (loadPerPart[partId] > overloadedThreshold)
				overloadedParts.add(partId);
			else if (loadPerPart[partId] == overloadedThreshold)
				saturatedParts.add(partId);
		}
		
		// Find out the tx nodes on these parts
		List<TxNode> nodesOnOverloadedParts = new ArrayList<TxNode>();
		for (TxNode node : graph.getTxNodes()) { // this should be in the order of tx number
			int homePartId = node.getPartId();
			if (overloadedParts.contains(homePartId)) {
				nodesOnOverloadedParts.add(node);
			}
		}
		
		// Reverse the list, which makes the tx node ordered by tx number from large to small
		Collections.reverse(nodesOnOverloadedParts);
		
		return nodesOnOverloadedParts;
	}
	
	private List<TxNode> rerouteTxNodesToUnderloadedParts(List<TxNode> candidateTxNodes, int increaseTolerence) {
		List<TxNode> nextCandidates = new ArrayList<TxNode>();
		
		for (TxNode node : candidateTxNodes) {
			// Count remote edges (including write edges)
			int currentPartId = node.getPartId();
			
			// If the home partition is no longer a overloaded part, skip it
			if (!overloadedParts.contains(currentPartId))
				continue;
			
			int currentRemoteEdges = countRemoteReadWriteEdges(node, currentPartId);
			int bestDelta = increaseTolerence + 1;
			int bestPartId = currentPartId;
			
			// Find a better partition
			for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
				// Skip home partition
				if (partId == currentPartId)
					continue;
				
				// Skip overloaded partitions
				if (overloadedParts.contains(partId))
					continue;
				
				// Skip saturated partitions
				if (saturatedParts.contains(partId))
					continue;
				
				// Count remote edges
				int remoteEdgeCount = countRemoteReadWriteEdges(node, partId);
				
				// Calculate the difference
				int delta = remoteEdgeCount - currentRemoteEdges;
				if (delta <= increaseTolerence) {
					// Prefer the machine with lower loadn
					if ((delta < bestDelta) ||
							(delta == bestDelta && loadPerPart[partId] < loadPerPart[bestPartId])) {
						bestDelta = delta;
						bestPartId = partId;
					}
				}
			}
			
			// If there is no match, try next tx node
			if (bestPartId == currentPartId) {
				nextCandidates.add(node);
				continue;
			}
//			System.out.println(String.format("Find a better partition %d for tx.%d", bestPartId, node.getTxNum()));
			node.setPartId(bestPartId);
			
			// Update loads
			loadPerPart[currentPartId]--;
			if (loadPerPart[currentPartId] == overloadedThreshold) {
				overloadedParts.remove(currentPartId);
				saturatedParts.add(currentPartId);
			}	
			loadPerPart[bestPartId]++;
			if (loadPerPart[bestPartId] == overloadedThreshold) {
				saturatedParts.add(bestPartId);
			}
			
			// Check if there are still overloaded machines
			if (overloadedParts.isEmpty())
				return null;
		}
		
		return nextCandidates;
	}
	
	private int countRemoteReadWriteEdges(TxNode node, int homePartId) {
		int count = 0;
		
		for (Edge readEdge : node.getReadEdges()) {
			// Skip replicated records
			if (partMgr.isFullyReplicated(readEdge.getResourceKey()))
				continue;
			
			if (readEdge.getTarget().getPartId() != homePartId)
				count++;
		}
		
		for (Edge writeEdge : node.getWriteEdges()) {
			if (writeEdge.getTarget().getPartId() != homePartId)
				count++;
		}
		
		// Note: We do not consider write back edges because Hermes will make it local
		
		return count;
	}
}
