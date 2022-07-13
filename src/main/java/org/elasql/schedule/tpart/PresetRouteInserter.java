package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.graph.TGraph;

public class PresetRouteInserter implements BatchNodeInserter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = task.getRoute();
			
			if (route == StoredProcedureCall.NO_ROUTE)
				throw new RuntimeException("No route defined in tx." + task.getTxNum());
			
			graph.insertTxNode(task, route);
		}
	}
}
