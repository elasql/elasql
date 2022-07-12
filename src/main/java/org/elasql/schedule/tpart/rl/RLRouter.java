package org.elasql.schedule.tpart.rl;

import java.util.List;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.hermes.HermesNodeInserter;

public class RLRouter extends HermesNodeInserter {
	private static Logger logger = Logger.getLogger(RLRouter.class.getName());
	
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = task.getRoute();
			
			if (route == TPartStoredProcedureTask.NO_ROUTE) {
//				throw new RuntimeException("No route defined in tx." + task.getTxNum());
				insertAccordingRemoteEdges(graph, task);
			} else {
				graph.insertTxNode(task, route);
			}
			
		}
		// Debug: show the distribution of assigned masters
//		for (TxNode node : graph.getTxNodes())
//			assignedCounts[node.getPartId()]++;
//		reportRoutingDistribution(tasks.get(0).getArrivedTime());
	}
}
