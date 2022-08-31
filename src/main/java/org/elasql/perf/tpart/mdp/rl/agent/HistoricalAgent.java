package org.elasql.perf.tpart.mdp.rl.agent;

import java.util.logging.Logger;

import org.elasql.perf.tpart.mdp.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.mdp.rl.model.TrainedBCQ;
import org.elasql.perf.tpart.mdp.rl.util.Memory;
import org.elasql.perf.tpart.mdp.rl.util.MemoryCsvLoader;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;

public class HistoricalAgent extends RlAgent{
	private static Logger logger = Logger.getLogger(HistoricalAgent.class.getName());

	private Memory memory = new Memory(30_000);
	
	public Route suggestRoute (TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = getCurrentState(graph, task, metricWarehouse);
		return new Route(trainedAgent.react(state));
	}
	
	@Override
	public void train() {
		if (firstTime) {
			prepareAgent();
			firstTime = false;
		}
	}

	protected void prepareAgent() {
		memory = prepareTrainingData();
		System.out.println(memory.size());
		agent = new OfflineBCQ(64, 32, 32, 0.99f, 0.001f, memory);
		trainedAgent = new TrainedBCQ();
		updateAgent(episode);
	}

	private Memory prepareTrainingData() {
		return MemoryCsvLoader.loadCsvAsDataFrame();
	}
}
