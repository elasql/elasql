package org.elasql.perf.tpart.rl.agent;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.DQN;
import org.elasql.perf.tpart.rl.model.TrainedDQN;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.vanilladb.core.util.TransactionProfiler;

import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public class OnlineAgent extends Agent {
	private static Logger logger = Logger.getLogger(OnlineAgent.class.getName());

	private long startTrainTxNum = 30_000;

	public OnlineAgent() {
		prepareAgent();
	}
	
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
		} else {
			action = agent.react(state);
		}
		return action;
	}

	protected void prepareAgent() {
		agent = new DQN(64, 32, 32, 0.99f, 0.001f, memory);
		Elasql.taskMgr().runTask(trainer);
		trainedAgent = new TrainedDQN();
	}
	
	@Override
	protected void updateAgent(int episode) {
		long startTrainTime = System.nanoTime();
		while (episode > 0) {
			try (NDManager submanager = NDManager.newBaseManager().newSubManager()) {
				agent.updateModel(submanager);
				if (episode == 1) {
					System.out.print("RL model loss: ");
					System.out.println(agent.updateModel(submanager).toString());
				}
			} catch (TranslateException e) {
				e.printStackTrace();
			}
			episode--;
			// TODO : need a evaluate method?
		}
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Training finished!!"));
		
//		trainedAgent = new TrainedDQN();
		trainedAgent.setPredictor(agent.takeoutPredictor());
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Setting predictor finished!!"));
		if (!prepared) {
			prepared = true;
		}
		System.out.println("Train time:" + (System.nanoTime()-startTrainTime));
	}
	
	private boolean isTrainTxNum(long txNum) {
		return txNum >= startTrainTxNum && (txNum - startTrainTxNum) % 5_000 == 0;
	}
}
