package org.elasql.schedule.tpart.rl;

import java.util.List;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.hermes.HermesNodeInserter;

public class PresetOrHermesRouter extends HermesNodeInserter {
	private static Logger logger = Logger.getLogger(PresetOrHermesRouter.class.getName());
	
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = task.getRoute();
			
			if (route == StoredProcedureCall.NO_ROUTE) {
				insertAccordingRemoteEdges(graph, task);
			} else {
				graph.insertTxNode(task, route);
			}
			
		}
	}
}
