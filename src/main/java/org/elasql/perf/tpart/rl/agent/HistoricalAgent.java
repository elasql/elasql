package org.elasql.perf.tpart.rl.agent;

import java.util.logging.Logger;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.rl.model.TrainedBCQ;
import org.elasql.perf.tpart.rl.util.Memory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.util.CsvLoader;

public class HistoricalAgent extends Agent{
	private static Logger logger = Logger.getLogger(HistoricalAgent.class.getName());

	private Memory memory = new Memory(30_000);
	
	public int react (TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = prepareState(graph, task, metricWarehouse);
		int action = trainedAgent.react(state);
		return action;
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
		return CsvLoader.loadCsvAsDataFrame();
	}
}
