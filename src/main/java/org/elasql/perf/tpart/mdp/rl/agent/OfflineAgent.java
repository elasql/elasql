package org.elasql.perf.tpart.mdp.rl.agent;

import org.elasql.perf.tpart.mdp.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.mdp.rl.model.TrainedBCQ;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;

public class OfflineAgent extends RlAgent {
	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = getCurrentState(graph, task, metricWarehouse);

		Route action = null;
		if (needToTrainNow(task.getTxNum())) {
			train();
		} 
		
		if (prepared) {
			action = new Route(trainedAgent.react(state));
		}
		
		return action;
	}

	protected void prepareAgent() {
		agent = new OfflineBCQ(256, 32, 32, OnlineAgent.DISCOUNT_RATE, OnlineAgent.LEARNING_RATE, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedBCQ();
	}
	
}
