package org.elasql.perf.tpart.mdp.rl.agent;

import java.util.logging.Logger;

import org.elasql.perf.tpart.mdp.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.mdp.rl.model.TrainedBCQ;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.vanilladb.core.util.TransactionProfiler;

public class FullyOfflineAgent extends RlAgent {
	private static Logger logger = Logger.getLogger(FullyOfflineAgent.class.getName());
	
	private long startTrainTxNum = 150_000;
	private boolean trained = false;

	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		TransactionProfiler.getLocalProfiler().startComponentProfiler("Prepare state");
		float[] state = getCurrentState(graph, task, metricWarehouse);
		TransactionProfiler.getLocalProfiler().stopComponentProfiler("Prepare state");
		Route action = null;
//		if(task.getTxNum() == startTrainTxNum) {
		if(!trained && System.currentTimeMillis() - startTime > 90_000) {
			System.out.println("train");
			train();
			trained = true;
		} 
		if (prepared) {
			TransactionProfiler.getLocalProfiler().startComponentProfiler("Base React");
				action = new Route(trainedAgent.react(state));
				TransactionProfiler.getLocalProfiler().stopComponentProfiler("Base React");
		}
		return action;
	}
	
	protected void prepareAgent() {
		agent = new OfflineBCQ(64, 32, 32, 0.99f, 0.001f, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedBCQ();	
	}
}
