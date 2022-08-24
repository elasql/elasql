package org.elasql.perf.tpart.rl.agent;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	public static final int ACTION_DIM = PartitionMetaMgr.NUM_PARTITIONS;
	public static final int STATE_DIM = PartitionMetaMgr.NUM_PARTITIONS * 3;

	public static final int REWARD_TYPE;

	static {
		REWARD_TYPE = ElasqlProperties.getLoader().getPropertyAsInteger(Agent.class.getName() + ".REWARD_TYPE", 1);
	}

	private static Logger logger = Logger.getLogger(Agent.class.getName());

	protected BaseAgent agent;
	protected TrainedAgent trainedAgent;

	protected int episode = 2_000;
	protected Memory memory = new Memory(20_000);
	protected Trainer trainer = new Trainer();
	protected Map<Long, float[]> cachedStates = new ConcurrentHashMap<Long, float[]>();
	protected Map<Long, int[]> cachedRemote = new ConcurrentHashMap<Long, int[]>();
	protected Map<Long, Float> cachedReward = new ConcurrentHashMap<Long, Float>();

	private boolean isEval = false;
	protected boolean prepared = false;
	protected boolean firstTime = true;

	private float[] loadPerPart = new float[ACTION_DIM];
	private float[] recordPerPart = new float[ACTION_DIM];

	private Queue<Integer> loadHistory;
//	private Queue<float[]> recordHistory;
	private static final int WINDOW_SIZE = 100;
	
	protected long startTrainTxNum;
	protected int trainingPeriod;

	protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public class Trainer extends Task {
		private boolean train = false;

		@Override
		public void run() {
			Thread.currentThread().setName("agent-trainer");

			while (true) {
				if (train) {
					episode = 100;
					updateAgent(episode);
					train = false;
				} else {
					try {
						Thread.sleep(5_000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}

		public void train() {
			train = true;
		}
	}

	public abstract int react(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse);

	public Agent() {
		loadLib();
		loadHistory = new ArrayDeque<Integer>();
	}

	private void loadLib() {
		NDManager.newBaseManager();
	}

	public void train() {
		if (firstTime) {
			Elasql.taskMgr().runTask(new Task() {
				@Override
				public void run() {
					Thread.currentThread().setName("prepare-agent");
					prepareAgent();
					System.out.println("prepare finished!");
				}
			});
			firstTime = false;
		}
		trainer.train();
	}

	public void cacheTxState(long txNum, float[] state) {
		cachedStates.put(txNum, state);
	}

	public void cacheRemote(long txNum, int[] remote) {
		if (!isEval) {
			cachedRemote.put(txNum, remote);
		}
	}

	public void onTxCommit(long txNum, int masterId, long latency) {
		float[] state = cachedStates.remove(txNum);
		if (state == null)
			throw new RuntimeException("Cannot find cached state for tx." + txNum);
		if (!isEval) {
			float reward = cachedReward.remove(txNum);
			reward = REWARD_TYPE == 0 ? calReward(latency) : reward;
			memory.setStep(txNum, state, masterId, reward, false);
		}
	}

	protected int[] countRemote(TGraph graph, TPartStoredProcedureTask task) {
		int[] remote = new int[ACTION_DIM];
		for (int partId = 0; partId < ACTION_DIM; partId++) {
			remote[partId] = countRemoteReadEdge(graph, task, partId);
		}
		return remote;
	}

	private int countRemoteReadEdge(TGraph graph, TPartStoredProcedureTask task, int partId) {
		int remoteEdgeCount = 0;

		for (PrimaryKey key : task.getReadSet()) {
			// Skip replicated records
			if (Elasql.partitionMetaMgr().isFullyReplicated(key))
				continue;

			if (graph.getResourcePosition(key).getPartId() != partId) {
				remoteEdgeCount++;
			}
		}

		return remoteEdgeCount;
	}

//	public void onTxRouting(long txNum, int partId) {
//		loadPerPart[partId]++;
//		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
//			loadPerPart[nodeId] -= 1 / PartitionMetaMgr.NUM_PARTITIONS;
//		}
//
//		float[] txWriteSet = cachedStates.get(txNum);
//		recordPerPart[partId] += txWriteSet[partId];
//
//		if (!isEval) {
////			System.out.println(calReward());
//			cachedReward.put(txNum, calReward(txNum, partId, txWriteSet));
//		}
//	}

	public void onTxRouting(long txNum, int partId) {
		loadHistory.add(partId);

		if (loadHistory.size() > WINDOW_SIZE)
			loadHistory.remove();

		float[] state = cachedStates.get(txNum);

		// record history
//		float[] writeSet = new float[ACTION_DIM];
//		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
//			if(nodeId != partId) {
//				writeSet[partId] += state[nodeId];
//			}
//		}
//		recordHistory.add(writeSet);
//
//		if (recordHistory.size() > WINDOW_SIZE)
//			recordHistory.remove();

		if (!isEval) {
//			System.out.println(calReward());
			cachedReward.put(txNum, calReward(txNum, partId, state));
		}
	}

	public float[] prepareState(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		float[] state = new float[STATE_DIM];

		// write record state
		int[] count = extractLocalDistribution(task.getUpdateSet(), graph);
		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
			state[nodeId] = (float) count[nodeId];
		}

		// Tx distribution
//		long txCount = task.getTxNum() + 1;
//		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
//			state[nodeId + ACTION_DIM] = (float) (loadPerPart[nodeId] / txCount) > 0.5? 1:0;
//		}
		loadPerPart = new float[ACTION_DIM];
		for (int load : loadHistory) {
			loadPerPart[load] += 1;
		}
		float load = 0.0f;
		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
			load = (float) (loadPerPart[nodeId] / loadHistory.size());
			state[nodeId + ACTION_DIM] = load > 0.5 ? 1 : load < 0.3 ? -1 : 0;
		}
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
		lock.writeLock().lock();
		try {
			trainedAgent.setPredictor(agent.takeoutPredictor(), ((OfflineBCQ) agent).takeoutImitationPredictor());
		} finally {
			lock.writeLock().unlock();
		}

		if (!prepared) {
			prepared = true;
		}
	}
	
	protected boolean isTrainTxNum(long txNum) {
		return txNum >= startTrainTxNum && (txNum - startTrainTxNum) % trainingPeriod == 0;
	}

	private float calReward() {
		double minLoad = Double.MAX_VALUE;
		double maxLoad = Double.MIN_VALUE;
		for (double load : loadPerPart) {
			if (load < minLoad)
				minLoad = load;
			if (load > maxLoad)
				maxLoad = load;
		}
		double loadDiff = maxLoad - minLoad;
		return (float) (1000 / loadDiff);
	}

	private float calReward(long txNum, int partId, float[] state) {
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
		for (int nodeId = 0; nodeId < ACTION_DIM; nodeId++) {
			if (maxRecordCount < state[nodeId])
				maxRecordCount = state[nodeId];
		}
		float readRecordScore = state[partId] / maxRecordCount;
//		readRecordScore = (float) Math.pow(2, readRecordScore);

		// Load balancing score
		float loadBalScore = 1 - state[partId + ACTION_DIM];

		// Record balancing score
//		float recordBalScore = 1 - state[partId + ACTION_DIM + ACTION_DIM];

//				float reward = readRecordScore * 0.5f + loadBalScore * 0.5f;
		float reward = readRecordScore + loadBalScore;

		return reward;
	}

	private float calReward(long latency) {
		// avg is 100ms
		return (float) 100.0f / (float) latency;
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
