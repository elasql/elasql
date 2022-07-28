package org.elasql.perf.tpart.rl.agent;

import java.util.logging.Logger;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.rl.model.TrainedBCQ;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.vanilladb.core.util.TransactionProfiler;

public class FullyOfflineAgent extends Agent{
	private static Logger logger = Logger.getLogger(FullyOfflineAgent.class.getName());
	
	private long startTrainTxNum = 150_000;

	public int react(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		TransactionProfiler.getLocalProfiler().startComponentProfiler("Prepare state");
		float[] state = prepareState(graph, task, metricWarehouse);
		int[] remote = countRemote(graph, task);
		TransactionProfiler.getLocalProfiler().stopComponentProfiler("Prepare state");
		int action = StoredProcedureCall.NO_ROUTE;
		// cache state for reward
		cacheTxState(task.getTxNum(), state);
		if (task.getTxNum() < startTrainTxNum) {
			cacheRemote(task.getTxNum(), remote);
		} else if(task.getTxNum() == startTrainTxNum) {
			train();
			eval();
		} else if (prepared) {
			TransactionProfiler.getLocalProfiler().startComponentProfiler("Base React");
				action = trainedAgent.react(state);
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
