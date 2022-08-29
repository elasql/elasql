package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;

public class PresetRouter implements BatchNodeInserter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			Route route = task.getRoute();
			
			if (route == null)
				throw new RuntimeException("No route defined in tx." + task.getTxNum());
			
			graph.insertTxNode(task, route.getDestination());
		}
	}
}
