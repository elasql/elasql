package org.elasql.perf.tpart.rl.agent;

import java.util.logging.Logger;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.rl.model.TrainedBCQ;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;

public class OfflineAgent extends Agent {
	private static Logger logger = Logger.getLogger(OfflineAgent.class.getName());

	public Route react(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = prepareState(graph, task, metricWarehouse);

		Route action = null;
		cacheTxState(task.getTxNum(), state);
		if (isTrainTxNum(task.getTxNum())) {
			train();
		} 
		if (prepared) {
			action = new Route(trainedAgent.react(state));
		}
		return action;
	}

	protected void prepareAgent() {
		agent = new OfflineBCQ(64, 32, 32, 0.99f, 0.001f, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedBCQ();
	}
	
}
