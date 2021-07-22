package org.elasql.schedule.tpart;

import java.util.ArrayList;
import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class LocalFirstNodeInserter implements BatchNodeInserter {
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	private List<Integer> ties = new ArrayList<Integer>();

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			insertAccordingRemoteEdges(graph, task);
		}
	}
	
	private void insertAccordingRemoteEdges(TGraph graph, TPartStoredProcedureTask task) {
		int bestPartId = 0;
		int minRemoteEdgeCount = task.getReadSet().size();
		ties.clear();
		
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			
			// Count the number of remote edge
			int remoteEdgeCount = countRemoteReadEdge(graph, task, partId);
			
			// Find the node in which the tx has fewest remote edges.
			if (remoteEdgeCount < minRemoteEdgeCount) {
				minRemoteEdgeCount = remoteEdgeCount;
				bestPartId = partId;
				ties.clear();
				ties.add(partId);
			} else if (remoteEdgeCount == minRemoteEdgeCount) {
				ties.add(partId);
			}
		}
		
		// Handle ties if there are some
		if (ties.size() > 1) {
			int chooseTiePart = (int) (task.getTxNum() % ties.size());
			bestPartId = ties.get(chooseTiePart);
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

}
