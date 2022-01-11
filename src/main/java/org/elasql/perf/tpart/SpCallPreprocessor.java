package org.elasql.perf.tpart;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.workload.FeatureExtractor;
import org.elasql.perf.tpart.workload.TransactionDependencyRecorder;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.perf.tpart.workload.TransactionFeaturesRecorder;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.client.BatchSpcSender;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.TPartScheduler;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
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
	
	// For collecting features
	private TransactionFeaturesRecorder featureRecorder;
	private TransactionDependencyRecorder dependencyRecorder;
	
	public SpCallPreprocessor(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, TGraph graph,
			boolean isBatching, TpartMetricWarehouse metricWarehouse,
			Estimator performanceEstimator) {
		
		// For generating execution plan and sp task
		this.factory = factory;
		this.inserter = inserter;
		this.graph = graph;
		this.isBatching = isBatching;
		this.performanceEstimator = performanceEstimator;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		// For collecting features
		featureExtractor = new FeatureExtractor(metricWarehouse);
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
				
				// Add normal SPs to the task batch
				if (task.getProcedureType() == ProcedureType.NORMAL) {
					// Pre-process the transaction 
					if (TPartPerformanceManager.ENABLE_COLLECTING_DATA ||
							performanceEstimator != null) {
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
		TransactionFeatures features = featureExtractor.extractFeatures(task, graph);
		
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
	}
	
	private void routeBatch(List<TPartStoredProcedureTask> batchedTasks) {
		// Insert the batch of tasks
		inserter.insertBatch(graph, batchedTasks);
		
		// add write back edges
		graph.addWriteBackEdge();
		
		// Clean up the tx nodes
		graph.clear();
	}
}
