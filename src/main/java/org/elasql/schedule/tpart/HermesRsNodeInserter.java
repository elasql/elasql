package org.elasql.schedule.tpart;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

/**
 * A variant of {@link HermesNodeInserter} that considers size of records.
 * 
 * @author yslin
 */
public class HermesRsNodeInserter implements BatchNodeInserter {
	
	private static final double IMBALANCED_TOLERANCE = HermesNodeInserter.IMBALANCED_TOLERANCE;
	private static final double EQUAL_EPSILON = 0.01;
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	private int[] loadPerPart = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private Set<Integer> overloadedParts = new HashSet<Integer>();
	private Set<Integer> saturatedParts = new HashSet<Integer>();
	private int overloadedThreshold;
	private List<Integer> ties = new ArrayList<Integer>();

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
		double minRemoteEdgeCost = Double.MAX_VALUE;
		ties.clear();
		
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			
			// Count the number of remote edge
			double remoteEdgeCost = calculateRemoteReadCost(graph, task, partId);
			
			// Find the node in which the tx has fewest remote edges.
			if (remoteEdgeCost < minRemoteEdgeCost) {
				minRemoteEdgeCost = remoteEdgeCost;
				bestPartId = partId;
				ties.clear();
				ties.add(partId);
			} else if (Math.abs(remoteEdgeCost - minRemoteEdgeCost) < EQUAL_EPSILON) { // means equals
				ties.add(partId);
			}
		}
		
		// Handle ties if there are some
		if (ties.size() > 1) {
			int chooseTiePart = (int) (task.getTxNum() % ties.size());
			bestPartId = ties.get(chooseTiePart);
		}
		
		graph.insertTxNode(task, bestPartId);
		
		loadPerPart[bestPartId]++;
	}
	
	private int calculateRemoteReadCost(TGraph graph, TPartStoredProcedureTask task, int partId) {
		int cost = 0;
		
		for (RecordKey key : task.getReadSet()) {
			// Skip replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			if (graph.getResourcePosition(key).getPartId() != partId) {
				cost += getMovingCost(key);
			}
		}
		
		return cost;
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
			
			double currentRemoteCost = calculateRemoteReadWriteCost(node, currentPartId);
			double bestDelta = increaseTolerence + 1.0;
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
				double remoteCost = calculateRemoteReadWriteCost(node, partId);
				
				// Calculate the difference
				double delta = remoteCost - currentRemoteCost;
				if (delta < increaseTolerence + EQUAL_EPSILON) {
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
	
	private double calculateRemoteReadWriteCost(TxNode node, int homePartId) {
		double cost = 0.0;
		
		for (Edge readEdge : node.getReadEdges()) {
			// Skip replicated records
			if (partMgr.isFullyReplicated(readEdge.getResourceKey()))
				continue;
			
			if (readEdge.getTarget().getPartId() != homePartId)
				cost += getMovingCost(readEdge.getResourceKey());
		}
		
		for (Edge writeEdge : node.getWriteEdges()) {
			if (writeEdge.getTarget().getPartId() != homePartId)
				cost += getMovingCost(writeEdge.getResourceKey());
		}
		
		// Note: We do not consider write back edges because Hermes will make it local
		
		return cost;
	}
	
	private double getMovingCost(RecordKey key) {
		// Hardcode for YCSB Multi-Table
		int tableId = Integer.parseInt(key.getTableName().substring(5));
		// {0, 1} -> 0.2, {2, 3} -> 0.4, {4, 5} -> 0.6 ...
		return ((tableId / 2) + 1) * 0.2;
	}
}
