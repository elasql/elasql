package org.elasql.schedule.tpart.rl;

import java.util.List;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.hermes.HermesNodeInserter;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class PresetOrHermesRouter extends HermesNodeInserter {
	private static Logger logger = Logger.getLogger(PresetOrHermesRouter.class.getName());
	private int sameCount = 0;
	private int differentCount = 0;
	
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = task.getRoute();
			
//			if (route == StoredProcedureCall.NO_ROUTE) {
			int bestPartId = insertAccordingRemoteEdges(graph, task);
//			} else {
			if (route != StoredProcedureCall.NO_ROUTE) {
				graph.insertTxNode(task, route);
				if(bestPartId == route) {
					sameCount++;
				} else {
					differentCount++;
				}
				if (task.getTxNum() % 10_000 == 0) {
					System.out.println("RL : " + route);
					System.out.println("Same count : " + sameCount);
					System.out.println("Different count : " + differentCount);
				}
			}
			
		}
		// Debug: show the distribution of assigned masters
		for (TxNode node : graph.getTxNodes())
			assignedCounts[node.getPartId()]++;
		reportRoutingDistribution(tasks.get(0).getArrivedTime());
	}
}
