package org.elasql.perf.tpart;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.rl.agent.Agent;
import org.elasql.perf.tpart.workload.FeatureExtractor;
import org.elasql.perf.tpart.workload.TransactionDependencyRecorder;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.perf.tpart.workload.TransactionFeaturesRecorder;
import org.elasql.perf.tpart.workload.time.TimeRelatedFeatureMgr;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.client.BatchSpcSender;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.TPartScheduler;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.server.task.Task;

/**
 * A collector that collects the features of transactions.
 * 
 * @author Yu-Shan Lin
 */
public class SpCallPreprocessor extends Task {
	
	private BlockingQueue<StoredProcedureCall> spcQueue;
	private FeatureExtractor featureExtractor;

	// Components to simulate the scheduler
	private TPartStoredProcedureFactory factory;
	private BatchNodeInserter inserter;
	private TGraph graph;
	private boolean isBatching;
	private Estimator performanceEstimator;
	private Agent agent;
	private HashSet<PrimaryKey> keyHasBeenRead = new HashSet<PrimaryKey>();
	
	// XXX: Cache last tx's routing destination
	private int lastTxRoutingDest = -1;
	
	// for rl 
	private long startTrainTxNum = 150_000;
	private HashMap<Long, Long> latencyHistory = new HashMap<Long, Long>();
	
	// For collecting features
	private TpartMetricWarehouse metricWarehouse;
	private TransactionFeaturesRecorder featureRecorder;
	private TransactionDependencyRecorder dependencyRecorder;
	private TimeRelatedFeatureMgr timeRelatedFeatureMgr;
	
	public SpCallPreprocessor(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, TGraph graph,
			boolean isBatching, TpartMetricWarehouse metricWarehouse,
			Estimator performanceEstimator, Agent agent) {
		
		// For generating execution plan and sp task
		this.factory = factory;
		this.inserter = inserter;
		this.graph = graph;
		this.isBatching = isBatching;
		this.performanceEstimator = performanceEstimator;
		this.agent = agent;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		// For collecting features
		this.metricWarehouse = metricWarehouse;
		timeRelatedFeatureMgr = new TimeRelatedFeatureMgr();
		featureExtractor = new FeatureExtractor(metricWarehouse, timeRelatedFeatureMgr);
		if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
			featureRecorder = new TransactionFeaturesRecorder();
			featureRecorder.startRecording();
			dependencyRecorder = new TransactionDependencyRecorder();
			dependencyRecorder.startRecording();
		}
	}

	public void preprocessSpCall(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall())
			spcQueue.add(spc);
	}
	
	public void onTransactionCommit(long txNum, int masterId) {
		// collect rl's action and reward
		if (agent != null && !agent.isEval()) {
			agent.collectAction(txNum, masterId);
			agent.collectReward(txNum, calReward(txNum), false);
		}
		
		featureExtractor.onTransactionCommit(txNum);
		timeRelatedFeatureMgr.onTxCommit(masterId);
	}

	@Override
	public void run() {
		List<TPartStoredProcedureTask> batchedTasks = 
				new ArrayList<TPartStoredProcedureTask>();
		List<Serializable> sendingList = new ArrayList<Serializable>();
		
		Thread.currentThread().setName("sp-call-preprocessor");
		
		while (true) {
			try {
				// Take a SP call
				StoredProcedureCall spc = spcQueue.take();
				
				// Add to the sending list
				sendingList.add(spc);
				
				// Get the read-/write-set by creating a SP task
				TPartStoredProcedureTask task = createSpTask(spc);
				
				// Record start time to calculate latency for rl agent
				latencyHistory.put(task.getTxNum(), System.nanoTime());
				
				// Add normal SPs to the task batch
				if (task.getProcedureType() == ProcedureType.NORMAL ||
						task.getProcedureType() == ProcedureType.CONTROL) {
					// Pre-process the transaction 
					if (TPartPerformanceManager.ENABLE_COLLECTING_DATA ||
							performanceEstimator != null || agent != null) {
						preprocess(spc, task);
					}
					
					// Add to the schedule batch
					batchedTasks.add(task);
				}
				
				if (sendingList.size() >= BatchSpcSender.COMM_BATCH_SIZE) {
					// Send the SP call batch to total ordering
					Elasql.connectionMgr().sendTotalOrderRequest(sendingList);
					sendingList = new ArrayList<Serializable>();
				}
				
				// Process the batch as TPartScheduler does
				if ((isBatching && batchedTasks.size() >= TPartScheduler.SCHEDULE_BATCH_SIZE)
						|| !isBatching) {
					// Route the task batch
					routeBatch(batchedTasks);
					batchedTasks.clear();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private TPartStoredProcedureTask createSpTask(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall()) {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(spc.getPid(), spc.getTxNum());
			sp.prepare(spc.getPars());
			return new TPartStoredProcedureTask(spc.getClientId(), spc.getConnectionId(),
					spc.getTxNum(), spc.getArrivedTime(), null, sp, null);
		}
		
		return null;
	}
	
	private void preprocess(StoredProcedureCall spc, TPartStoredProcedureTask task) {
		if (task.getProcedureType() == ProcedureType.CONTROL)
			return;
		
		TransactionFeatures features = featureExtractor.extractFeatures(task, graph, keyHasBeenRead, lastTxRoutingDest);
		
		// records must be read from disk if they are never read.
		bookKeepKeys(task);
		
		// Record the feature if necessary
		if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
			featureRecorder.record(features);
			dependencyRecorder.record(features);
		}
		
		// Estimate the performance if necessary
		if (performanceEstimator != null) {
			TransactionEstimation estimation = performanceEstimator.estimate(features);
			
			// Save the estimation
			spc.setMetadata(estimation.toBytes());
			task.setEstimation(estimation);
		}
		
		if (agent != null) {
			if (task.getTxNum() < startTrainTxNum) {
				agent.collectState(task.getTxNum(), agent.prepareState(graph, task, metricWarehouse));
			} else if(task.getTxNum() == startTrainTxNum) {
				agent.train();
			} else if (agent.isprepare()) {	
				System.out.println(task.getTxNum());
				int route = agent.react(graph, task, metricWarehouse);
				task.setRoute(route);	
			}
		}
	}
	
	private void routeBatch(List<TPartStoredProcedureTask> batchedTasks) {
		// Insert the batch of tasks
		inserter.insertBatch(graph, batchedTasks);
		
		lastTxRoutingDest = timeRelatedFeatureMgr.pushInfo(graph);
		
		// Notify the estimator where the transactions are routed
		if (performanceEstimator != null) {
			for (TxNode node : graph.getTxNodes()) {
				performanceEstimator.notifyTransactionRoute(node.getTxNum(), node.getPartId());
			}
		}
		
		// add write back edges
		graph.addWriteBackEdge();
		
		// Clean up the tx nodes
		graph.clear();
	}
	
	private void bookKeepKeys(TPartStoredProcedureTask task) {
		for (PrimaryKey key : task.getReadSet()) {
			keyHasBeenRead.add(key);
		}
	}
	
	private float calReward(long txNum) {
		long endTime = System.nanoTime();
		long latency = endTime - latencyHistory.get(txNum);
		latencyHistory.remove(txNum);
		return (float) 1/latency;
	}
}
