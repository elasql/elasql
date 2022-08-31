package org.elasql.perf.tpart.mdp;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class TransactionRoutingEnvironment {
	
	public static final int STATE_DIM = PartitionMetaMgr.NUM_PARTITIONS * 2;
	public static final int ACTION_DIM = PartitionMetaMgr.NUM_PARTITIONS;
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	private static final int WINDOW_SIZE = 100;
	
	private static final int LOAD_UNDERLOAD = -1;
	private static final int LOAD_NORMAL = 0;
	private static final int LOAD_OVERLOAD = 1;

	public static final int REWARD_TYPE;

	static {
		REWARD_TYPE = ElasqlProperties.getLoader().getPropertyAsInteger(
				TransactionRoutingEnvironment.class.getName() + ".REWARD_TYPE", 1);
	}
	
	private int[] machineTxCounts = new int[NUM_PARTITIONS];
	private Queue<Integer> routeHistory = new ArrayDeque<Integer>();
	
	private long lastTxNum = -1;

	public State getCurrentState(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		if (task.getTxNum() < lastTxNum)
			throw new RuntimeException(String.format(
					"Get an older transaction (tx.%d) that the last one (tx.%d)",
					task.getTxNum(), lastTxNum));
		lastTxNum = task.getTxNum();
		
		int[] localReadCounts = calcLocalKeyCounts(task.getReadSet(), graph);
		int[] machineLoads = calcMachineLoads();

		// Tx distribution
//		long txCount = task.getTxNum() + 1;
//		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
//			state[nodeId + ACTION_DIM] = (float) (loadPerPart[nodeId] / txCount) > 0.5? 1:0;
//		}
		// node written record distribution
//		float writtenCount = 0.0f;
//		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
//			writtenCount += recordPerPart[nodeId];
//		}
//		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
//			state[nodeId + ACTION_DIM + ACTION_DIM] = (float) (recordPerPart[nodeId] / writtenCount) > 0.5? 1:0;
//		}
//		float writtenCount = 0.0f;
//		recordPerPart = new float[ACTION_DIM];
//		for (float[] records : recordHistory) {
//			for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
//				recordPerPart[nodeId] += records[nodeId];
//				writtenCount += records[nodeId];
//			}
//		}
//		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
//			state[nodeId + ACTION_DIM + ACTION_DIM] = (float) (recordPerPart[nodeId] / writtenCount) > 0.5? 1:0;
//		}

		// node's CPU state
//		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
//			// there is another method : metricWarehouse.getSystemCpuLoad(nodeId)
//			double observation = (float) metricWarehouse.getAveragedSystemCpuLoad(nodeId, PERIOD);
//			observation = observation / CPU_MAX_CAPACITIES[nodeId];
//			observation = Math.min(observation, 1.0);
//			state[nodeId + PartitionMetaMgr.NUM_PARTITIONS] = (float) observation;
//		}

		return new State(localReadCounts, machineLoads);
	}
	
	public void onTxRouted(long txNum, int routeDest) {
		if (txNum != lastTxNum)
			throw new RuntimeException(String.format(
					"The routed transaction (tx.%d) is not the previous one (tx.%d)",
					txNum, lastTxNum));
		
		updateMachineLoads(txNum, routeDest);
	}
	
	public float calcReward(State state, int routeDest, long txLatency) {
		if (REWARD_TYPE == 0) {
			return calcRewardByLatency(txLatency);
		} else {
			return calcRewardByState(state, routeDest);
		}
	}

	private int[] calcLocalKeyCounts(Set<PrimaryKey> keys, TGraph graph) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int[] counts = new int[NUM_PARTITIONS];
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
		int[] newCounts = new int[NUM_PARTITIONS];
		for (int partId = 0; partId < newCounts.length; partId++) {
			newCounts[partId] = counts[partId] + fullyRepCount;
		}

		return newCounts;
	}
	
	private void updateMachineLoads(long txNum, int routeDest) {
		routeHistory.add(routeDest);
		machineTxCounts[routeDest]++;
		if (routeHistory.size() > WINDOW_SIZE) {
			int removedRoute = routeHistory.remove();
			machineTxCounts[removedRoute]--;
		}
	}
	
	private int[] calcMachineLoads() {
		int[] machineLoads = new int[NUM_PARTITIONS];
		int totalTxCount = routeHistory.size();
		
		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
			float normalizedLoad = 0.0f;
			if (totalTxCount > 0) {
				normalizedLoad = ((float) machineTxCounts[partId]) / totalTxCount;
			}
			machineLoads[partId] = normalizedLoad > 0.5 ? LOAD_OVERLOAD : normalizedLoad < 0.3 ? LOAD_UNDERLOAD : LOAD_NORMAL;
		}
		
		return machineLoads;
	}
	
	private float calcRewardByState(State state, int routeDest) {
//		double minLoad = Double.MAX_VALUE;
//		double maxLoad = Double.MIN_VALUE;
//		for (double load : loadPerPart) {
//			if (load < minLoad)
//				minLoad = load;
//			if (load > maxLoad)
//				maxLoad = load;
//		}
//		double loadDiff = maxLoad - minLoad;
//		int[] remote = cachedRemote.remove(txNum);
//		return (float) ( -remote[nodeId] - loadDiff);

		// Read record score
		float maxRecordCount = 0;
		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
			if (maxRecordCount < state.getLocalRead(partId))
				maxRecordCount = state.getLocalRead(partId);
		}
		float readRecordScore = state.getLocalRead(routeDest) / maxRecordCount;
//		readRecordScore = (float) Math.pow(2, readRecordScore);

		// Load balancing score
		float loadBalScore = 1 - (float) state.getMachineLoad(routeDest);

		// Record balancing score
//		float recordBalScore = 1 - state[partId + ACTION_DIM + ACTION_DIM];

//				float reward = readRecordScore * 0.5f + loadBalScore * 0.5f;
		float reward = readRecordScore + loadBalScore;

		return reward;
	}
	
	private float calcRewardByLatency(long latency) {
		// avg is 100ms
		return (float) 100.0f / (float) latency;
	}
}
