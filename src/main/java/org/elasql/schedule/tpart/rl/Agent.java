package org.elasql.schedule.tpart.rl;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.control.RoutingControlActuator;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.model.BaseAgent;
import org.elasql.perf.tpart.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.rl.util.Memory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.PresetRouteInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.CsvLoader;
import org.elasql.util.ElasqlProperties;

import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public class Agent extends PresetRouteInserter {
	private static Logger logger = Logger.getLogger(Agent.class.getName());

	private static final long PERIOD = 5_000;

	private static final int RL_METHOD;
	private static final double[] CPU_MAX_CAPACITIES;

	static {
		RL_METHOD = ElasqlProperties.getLoader().getPropertyAsInteger(Agent.class.getName() + ".RL_METHOD", 0);

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

	private BaseAgent agent;
	private TpartMetricWarehouse metricWarehouse;

	// Debug: show the distribution of assigned masters
	private long lastReportTime = -1;
	private int[] assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];

	public Agent(TpartMetricWarehouse metricWarehouse) {
		this.metricWarehouse = metricWarehouse;
		
		prepareAgent();	
	}

	private void prepareAgent() {
		Memory memory = null;
		switch (RL_METHOD) {
		case 0:
			memory = prepareTrainingData();
			System.out.println(memory.size());
			agent = new OfflineBCQ(64, 32, 32, 0.99f, 0.001f, memory);
			pretrain();
			break;
		default:
			throw new RuntimeException("Unsupport service rl method");
		}
	}

	private Memory prepareTrainingData() {
		return CsvLoader.loadCsvAsDataFrame();
	}

	private void pretrain() {
		int episode = 0;
		while (episode < 1000) {
			episode++;
			try (NDManager submanager = NDManager.newBaseManager().newSubManager()) {
				agent.updateModel(submanager);
			} catch (TranslateException e) {
				e.printStackTrace();
			}
			// TODO : need a evaluate method?
		}
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Training finished!!"));
	}
//
//	@Override
//	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
//		for (TPartStoredProcedureTask task : tasks) {
//			insert(graph, task);
//
//			// Debug: show the distribution of assigned masters
//			reportRoutingDistribution(task.getArrivedTime());
//		}
//	}
	
	public int react (TGraph graph, TPartStoredProcedureTask task) {
		float[] state = prepareState(graph, task);
		return agent.react(state);
	}

	private void insert(TGraph graph, TPartStoredProcedureTask task) {	
		float[] state = prepareState(graph, task);
		
		if (state == null)
			throw new IllegalArgumentException("there is no estimation for transaction " + task.getTxNum());

		// TODO : prepare state, i need a state mgr!!
		int bestMasterId = agent.react(state);

		// Debug
//		if (isPartition0Tx(task))
		assignedCounts[bestMasterId]++;

		graph.insertTxNode(task, bestMasterId);
	}

	private float[] prepareState(TGraph graph, TPartStoredProcedureTask task) {
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

	// Debug: show the distribution of assigned masters
	private void reportRoutingDistribution(long currentTime) {
		if (lastReportTime == -1) {
			lastReportTime = currentTime;
		} else if (currentTime - lastReportTime > 5_000_000) {
			StringBuffer sb = new StringBuffer();

			sb.append(String.format("Time: %d seconds - Routing: ", currentTime / 1_000_000));
			for (int i = 0; i < assignedCounts.length; i++) {
				sb.append(String.format("%d, ", assignedCounts[i]));
				assignedCounts[i] = 0;
			}
			sb.delete(sb.length() - 2, sb.length());

			System.out.println(sb.toString());

			lastReportTime = currentTime;
		}
	}
}
