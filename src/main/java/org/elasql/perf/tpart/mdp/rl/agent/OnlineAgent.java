package org.elasql.perf.tpart.mdp.rl.agent;

import java.util.Random;
import java.util.logging.Logger;

import org.elasql.perf.tpart.mdp.TransactionRoutingEnvironment;
import org.elasql.perf.tpart.mdp.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.mdp.rl.model.TrainedBCQ;
import org.elasql.perf.tpart.mdp.rl.util.ActionSampler;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;

public class OnlineAgent extends RlAgent {
	private static Logger logger = Logger.getLogger(OnlineAgent.class.getName());
	private final Random random = new Random(0);
	
	public OnlineAgent() {
		prepareAgent();
	}
	
	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = getCurrentState(graph, task, metricWarehouse);

		Route action = null;
		
		if (isTrainTxNum(task.getTxNum())) {
			train();
		} 
		if (prepared) {
			action = new Route(trainedAgent.react(state));
		} else {
			action = new Route(ActionSampler.random(TransactionRoutingEnvironment.ACTION_DIM, random));
		}
		return action;
	}

	protected void prepareAgent() {
		agent = new OfflineBCQ(64, 32, 32, 0.99f, 0.001f, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedBCQ();
	}
	
	@Override
	public void train() {
		trainer.train();
	}
}
