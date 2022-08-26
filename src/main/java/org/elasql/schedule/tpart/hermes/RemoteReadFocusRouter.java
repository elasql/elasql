package org.elasql.schedule.tpart.hermes;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class RemoteReadFocusRouter implements BatchNodeInserter {
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();

	// Debug: show the distribution of assigned masters
	private long lastReportTime = -1;
	private int[] assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			insertAccordingRemoteEdges(graph, task);
		}

		// Debug: show the distribution of assigned masters
		for (TxNode node : graph.getTxNodes())
			assignedCounts[node.getPartId()]++;
		reportRoutingDistribution(tasks.get(0).getArrivedTime());
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

	// Debug: show the distribution of assigned masters
	private void reportRoutingDistribution(long currentTime) {
		if (lastReportTime == -1) {
			lastReportTime = currentTime;
		} else if (currentTime - lastReportTime > 5_000_000) {
			StringBuffer sb = new StringBuffer();
			
			sb.append(String.format("Time: %d seconds - ", currentTime / 1_000_000));
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
