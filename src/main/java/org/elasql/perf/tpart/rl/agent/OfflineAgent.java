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

public class OfflineAgent extends Agent {
	private static Logger logger = Logger.getLogger(OfflineAgent.class.getName());

	private long startTrainTxNum = 100_000;

	public int react(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = prepareState(graph, task, metricWarehouse);

		int action = StoredProcedureCall.NO_ROUTE;
		cacheTxState(task.getTxNum(), state);
		if (isTrainTxNum(task.getTxNum())) {

			TransactionProfiler.getLocalProfiler().startComponentProfiler("Train");
			train();
			TransactionProfiler.getLocalProfiler().stopComponentProfiler("Train");
		} 
		if (prepared) {
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
	
	private boolean isTrainTxNum(long txNum) {
		return txNum >= startTrainTxNum && (txNum - startTrainTxNum) % 5_000 == 0;
	}
}
