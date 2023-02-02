package org.elasql.perf.tpart.mdp.rl.agent;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.CentralRoutingAgent;
import org.elasql.perf.tpart.mdp.State;
import org.elasql.perf.tpart.mdp.TransactionRoutingEnvironment;
import org.elasql.perf.tpart.mdp.rl.model.BaseAgent;
import org.elasql.perf.tpart.mdp.rl.model.OfflineBCQ;
import org.elasql.perf.tpart.mdp.rl.model.TrainedAgent;
import org.elasql.perf.tpart.mdp.rl.util.Memory;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.task.Task;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public abstract class RlAgent implements CentralRoutingAgent {
	private static Logger logger = Logger.getLogger(RlAgent.class.getName());

	private static final int MODEL_UPDATE_PERIDO; // in milliseconds
	private static final int MEMORY_SIZE;
	private static final int TRAINING_EPISODE;

	static {
		MODEL_UPDATE_PERIDO = ElasqlProperties.getLoader().getPropertyAsInteger(
				RlAgent.class.getName() + ".MODEL_UPDATE_PERIDO", 20_000);
		MEMORY_SIZE = ElasqlProperties.getLoader().getPropertyAsInteger(
				RlAgent.class.getName() + ".MEMORY_SIZE", 10000);
		TRAINING_EPISODE = ElasqlProperties.getLoader().getPropertyAsInteger(
				RlAgent.class.getName() + ".TRAINING_EPISODE", 100);
	}

	private static class Step {
		long txNum;
		float[] state;
		int action;
		float reward;
		boolean mask;

		public Step(long txNum, float[] state, int action, float reward, boolean mask) {
			this.txNum = txNum;
			this.state = state;
			this.action = action;
			this.reward = reward;
			this.mask = mask;
		}
	}

	protected BaseAgent agent;
	protected TrainedAgent trainedAgent;

	protected int episode = 2_000;
	protected Memory memory = new Memory(MEMORY_SIZE);
	protected Trainer trainer = new Trainer();

	protected Map<Long, State> cachedStates = new ConcurrentHashMap<Long, State>();

	private boolean isEval = false;
	private Random random = new Random();

	protected boolean prepared = false;
	protected boolean firstTime = true;

	protected long startTime, lastTrainTime;
	protected long startTrainTxNum = 30_000;
	protected int trainingPeriod = 5_000;

	protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	private TransactionRoutingEnvironment env = new TransactionRoutingEnvironment();
	private final BlockingQueue<Step> stepQueue = new LinkedBlockingQueue<Step>();

	public class Trainer extends Task {
		private boolean train = false;

		@Override
		public void run() {
			Thread.currentThread().setName("agent-trainer");

			while (true) {
				if (train) {
					episode = TRAINING_EPISODE;
					drainStepQueue();
					long startTime = System.nanoTime();
					updateAgent(episode);
					System.out.println(
							String.format("Training time: %d microseconds", (System.nanoTime() - startTime) / 1000));
					train = false;
				} else {
					try {
						Thread.sleep(MODEL_UPDATE_PERIDO / 10);
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

	public RlAgent() {
		loadLib();
		startTime = System.currentTimeMillis();
	}

	public abstract Route suggestRoute(TGraph graph, TPartStoredProcedureTask task,
			TpartMetricWarehouse metricWarehouse);

	private void loadLib() {
		NDManager.newBaseManager();
	}

	public void train() {
		if (firstTime) {

			Elasql.taskMgr().runTask(new Task() {
				@Override
				public void run() {
					Thread.currentThread().setName("prepare-agent");
					drainStepQueue();
					prepareAgent();
					System.out.println("prepare finished!");
				}
			});
			firstTime = false;
		}
		trainer.train();
	}

	private void drainStepQueue() {
		Step step = null;
		while ((step = stepQueue.poll()) != null) {
			int bound = (int) (step.txNum / 5_000) + 1;
			if (random.nextInt(bound) == 0)
				memory.setStep(step.txNum, step.state, step.action, step.reward, step.mask);
		}
	}

	public void onTxRouted(long txNum, int routeDest) {
		// For state transition
		env.onTxRouted(txNum, routeDest);
	}

	public void onTxCommitted(long txNum, int masterId, long latency) {
		if (!isEval) {
			State state = cachedStates.remove(txNum);

			if (state == null)
				throw new RuntimeException("Cannot find cached state for tx." + txNum);

			float reward = env.calcReward(state, masterId, latency);

			stepQueue.add(new Step(txNum, state.toFloatArray(), masterId, reward, false));
		}
	}

	public float[] getCurrentState(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		State state = env.getCurrentState(graph, task, metricWarehouse);

		if (!isEval) {
			// Cache the state for later training
			cachedStates.put(task.getTxNum(), state);
		}

		return state.toFloatArray();
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

	protected void updateAgent(int maxEpisode) {
//		LocalDateTime datetime = LocalDateTime.now();
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
//		String datetimeStr = datetime.format(formatter);

//		try (BufferedWriter log = new BufferedWriter(new FileWriter("training-" + datetimeStr + ".txt"))) {
		int ep = 0;
		while (ep < maxEpisode) {
			try (NDManager submanager = NDManager.newBaseManager().newSubManager()) {
				NDArray loss = agent.updateModel(submanager);
//					log.append(String.format("Episode: %d, Loss: %s", ep, Arrays.toString(loss.toFloatArray())));
//					log.newLine();
			} catch (TranslateException e) {
				e.printStackTrace();
			}
			ep++;
			// TODO : need a evaluate method?
		}
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}

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

	protected boolean needToTrainNow(long currentTxNum) {
		boolean isWarmUpCompleted = System.currentTimeMillis() - startTime > 90_000;
		if (isWarmUpCompleted) {
			if (lastTrainTime == 0) {
				lastTrainTime = System.currentTimeMillis();
				return true;
			} else {
				if (System.currentTimeMillis() - lastTrainTime > MODEL_UPDATE_PERIDO) {
					lastTrainTime = System.currentTimeMillis();
					return true;
				} else {
					return false;
				}
			}
		} else {
			return false;
		}
//		return txNum >= startTrainTxNum && (txNum - startTrainTxNum) % trainingPeriod == 0;
	}
}