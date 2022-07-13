package org.elasql.perf.tpart.rl.agent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.control.RoutingControlActuator;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.BaseAgent;
import org.elasql.perf.tpart.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.rl.model.TrainedAgent;
import org.elasql.perf.tpart.rl.util.Memory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.task.Task;

import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public abstract class Agent {
	private static Logger logger = Logger.getLogger(Agent.class.getName());

	private static final long PERIOD = 5_000;

	private static final double[] CPU_MAX_CAPACITIES;

	static {
		// TODO : duplicated
		// Gather CPU MAXs
		double[] cpuMaxCapacities = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int i = 0; i < cpuMaxCapacities.length; i++)
			cpuMaxCapacities[i] = 1.0;
		String cpuMaxStr = ElasqlProperties.getLoader().getPropertyAsString(
				RoutingControlActuator.class.getName() + ".CPU_MAX_CAPACITIES", "");
		if (!cpuMaxStr.isEmpty()) {
			String[] cpuMaxValues = cpuMaxStr.split(",");
			for (int i = 0; i < cpuMaxValues.length; i++) {
				double value = Double.parseDouble(cpuMaxValues[i].trim());
				cpuMaxCapacities[i] = value;
			}
		}
		CPU_MAX_CAPACITIES = cpuMaxCapacities;
	}

	protected BaseAgent agent;
	protected TrainedAgent trainedAgent;
	protected int episode = 1_000;
	protected Memory memory = new Memory(30_000);
	protected Trainer trainer = new Trainer();
	protected Map<Long, float[]> cachedStates = new ConcurrentHashMap<Long, float[]>();
	
	private boolean isEval = false;
	protected boolean prepared = false;
	protected boolean firstTime = true;

	public class Trainer extends Task {
		private boolean train = false;
		
		@Override
		public void run() {
			long startTime = System.nanoTime();
			while ((System.nanoTime() - startTime) / 1000_000 < 1_000) {
				if (train) {
					updateAgent(episode);
					break;
				}
				startTime = System.nanoTime();
			}	
			train = false;
		}
		
		public void train() {
			train = true;
		}
	}
	
	public abstract int react(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse);
	
	public void train() {
		if (firstTime) {
			prepareAgent();
			firstTime = false;
		}
		trainer.train();
	}
	
	public void cacheTxState(long txNum, float[] state) {
		if (!agent.isEval()) {
			cachedStates.put(txNum, state);
		}
	}
	
	public void onTxCommit(long txNum, int masterId, long latency) {
		if (!agent.isEval()) {
			float reward = calReward(latency);
			float[] state = cachedStates.remove(txNum);
			if (state == null)
				throw new RuntimeException("Cannot find cached state for tx." + txNum);
			memory.setStep(txNum, state, masterId, reward, false);
		}
	}

	public float[] prepareState(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = new float[PartitionMetaMgr.NUM_PARTITIONS * 2];
		
		// write record state
		int[] count = extractLocalDistribution(task.getUpdateSet(), graph);
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
				state[nodeId] = (float) count[nodeId];
		}
		
		// node's CPU state
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			// there is another method : metricWarehouse.getSystemCpuLoad(nodeId)
			double observation = (float) metricWarehouse.getAveragedSystemCpuLoad(nodeId, PERIOD);
			observation = observation / CPU_MAX_CAPACITIES[nodeId];
			observation = Math.min(observation, 1.0);
			state[nodeId + PartitionMetaMgr.NUM_PARTITIONS] = (float) observation;
		}
		
		return state;
	}
		
	public boolean isEval() {
		return isEval;
	}
	
	public boolean isprepare() {
		return prepared;
	}
	
	public void eval() {
		isEval = true;
	}
	
	protected abstract void prepareAgent();
	
	protected void updateAgent(int episode) {
		while (episode > 0) {
			try (NDManager submanager = NDManager.newBaseManager().newSubManager()) {
				agent.updateModel(submanager);
			} catch (TranslateException e) {
				e.printStackTrace();
			}
			episode--;
			// TODO : need a evaluate method?
		}
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Training finished!!"));
		
		trainedAgent.setPredictor(agent.takeoutPredictor(), ((OfflineBCQ) agent).takeoutImitationPredictor());
		if (!prepared) {
			prepared = true;
		}
	}
	
	private float calReward(long latency) {
		return (float) 1.0f / (float) latency;
	}
	
	// TODO : this method should not be here
	private int[] extractLocalDistribution(Set<PrimaryKey> keys, TGraph graph) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		int fullyRepCount = 0;
		
		// Count records
		for (PrimaryKey key : keys) {
			if (partMgr.isFullyReplicated(key)) {
				fullyRepCount++;
			} else {
				int partId = graph.getResourcePosition(key).getPartId();
				counts[partId]++;
			}
		}
		
		// Add fully replicated records
		int[] newCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		for (int partId = 0; partId < newCounts.length; partId++) {
			newCounts[partId] = counts[partId] + fullyRepCount;
		}
	    
		return newCounts;
	}
}
