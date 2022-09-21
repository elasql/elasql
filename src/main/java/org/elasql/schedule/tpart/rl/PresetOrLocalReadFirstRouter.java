package org.elasql.schedule.tpart.rl;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.LocalReadFirstRouter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;

public class PresetOrLocalReadFirstRouter extends LocalReadFirstRouter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			Route route = task.getRoute();
			
			if (route == null) {
				insertAccordingRemoteEdges(graph, task);
			} else {
				graph.insertTxNode(task, route.getDestination());
			}
		}
		
		// Debug: show the distribution of assigned masters
		for (TxNode node : graph.getTxNodes())
			assignedCounts[node.getPartId()]++;
		reportRoutingDistribution(tasks.get(0).getArrivedTime());
	}

	@Override
	public boolean needBatching() {
		return false;
	}
}
