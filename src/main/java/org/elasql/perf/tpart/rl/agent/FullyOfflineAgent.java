package org.elasql.perf.tpart.rl.agent;

import java.util.logging.Logger;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.rl.model.TrainedBCQ;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;

public class FullyOfflineAgent extends Agent{
	private static Logger logger = Logger.getLogger(FullyOfflineAgent.class.getName());
	
	private long startTrainTxNum = 150_000;

	public int react (TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = prepareState(graph, task, metricWarehouse);
		
		int action = -1;
		if (task.getTxNum() < startTrainTxNum) {
			this.collectState(task.getTxNum(), this.prepareState(graph, task, metricWarehouse));
		} else if(task.getTxNum() == startTrainTxNum) {
			train();
		} else if (prepared) {
			action = trainedAgent.react(state);
			task.setRoute(action);
		}
		return action;
	}
	
	protected void prepareAgent() {
		agent = new OfflineBCQ(64, 32, 32, 0.99f, 0.001f, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedBCQ();	
	}
}
