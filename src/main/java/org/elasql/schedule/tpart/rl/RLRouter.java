package org.elasql.schedule.tpart.rl;

import java.util.List;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.hermes.HermesNodeInserter;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class RLRouter extends HermesNodeInserter {
	private static Logger logger = Logger.getLogger(RLRouter.class.getName());

	private boolean prepared = false;

	// Debug: show the distribution of assigned masters
	private long lastReportTime = -1;
	private int[] assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = task.getRoute();
			
			if (route == TPartStoredProcedureTask.NO_ROUTE) {
//				throw new RuntimeException("No route defined in tx." + task.getTxNum());
				insertAccordingRemoteEdges(graph, task);
			} else {
				System.out.println("媽我在這！");
				graph.insertTxNode(task, route);
			}
			
		}
		// Debug: show the distribution of assigned masters
//		for (TxNode node : graph.getTxNodes())
//			assignedCounts[node.getPartId()]++;
//		reportRoutingDistribution(tasks.get(0).getArrivedTime());
	}
	
	private void hermesInserter(TGraph graph, List<TPartStoredProcedureTask> tasks) {
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

		// Step 3: Move tx nodes from overloaded machines to underloaded machines
		int increaseTolerence = 1;
		while (!overloadedParts.isEmpty()) {
			candidateTxNodes = rerouteTxNodesToUnderloadedParts(candidateTxNodes, increaseTolerence);
			increaseTolerence++;
			
			if (increaseTolerence > 100)
				throw new RuntimeException("Something wrong");
		}
	}
	
	private void rlInserter(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = task.getRoute();
			
			if (route == TPartStoredProcedureTask.NO_ROUTE)
				throw new RuntimeException("No route defined in tx." + task.getTxNum());
			
			graph.insertTxNode(task, route);
		}
	}

	// Debug: show the distribution of assigned masters
	private void reportRoutingDistribution(long currentTime) {
		if (lastReportTime == -1) {
			lastReportTime = currentTime;
		} else if (currentTime - lastReportTime > 5_000_000) {
			StringBuffer sb = new StringBuffer();

			sb.append(String.format("Time: %d seconds - Routing: ", currentTime / 1_000_000));
			for (int i = 0; i < assignedCounts.length; i++) {
				sb.append(String.format("%d, ", assignedCounts[i]));
				assignedCounts[i] = 0;
			}
			sb.delete(sb.length() - 2, sb.length());

			System.out.println(sb.toString());

			lastReportTime = currentTime;
		}
	}
}
