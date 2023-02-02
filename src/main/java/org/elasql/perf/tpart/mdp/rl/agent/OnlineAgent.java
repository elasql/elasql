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
import org.elasql.util.ElasqlProperties;

public class OnlineAgent extends RlAgent {
	private static Logger logger = Logger.getLogger(OnlineAgent.class.getName());
	private final Random random = new Random(0);
	
	public static final float EPSILON;
	public static final float LEARNING_RATE;
	public static final float DISCOUNT_RATE;
	
	static {
		EPSILON = (float) ElasqlProperties.getLoader().getPropertyAsDouble(
				OnlineAgent.class.getName() + ".EPSILON", 0.9);
		LEARNING_RATE = (float) ElasqlProperties.getLoader().getPropertyAsDouble(
				OnlineAgent.class.getName() + ".LEARNING_RATE", 0.001f);
		DISCOUNT_RATE = (float) ElasqlProperties.getLoader().getPropertyAsDouble(
				OnlineAgent.class.getName() + ".DISCOUNT_RATE", 0.99f);
	}
	
	public OnlineAgent() {
		prepareAgent();
	}
	
	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = getCurrentState(graph, task, metricWarehouse);

		Route action = null;
		
		if (needToTrainNow(task.getTxNum())) {
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
		agent = new OfflineBCQ(256, 32, 32, DISCOUNT_RATE, LEARNING_RATE, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedBCQ();
	}
	
	@Override
	public void train() {
		trainer.train();
	}
}
