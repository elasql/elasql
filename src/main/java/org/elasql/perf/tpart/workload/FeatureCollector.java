package org.elasql.perf.tpart.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.TPartScheduler;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.vanilladb.core.server.task.Task;

/**
 * A collector that collects the features of transactions.
 * 
 * @author Yu-Shan Lin
 */
public class FeatureCollector extends Task {
	
	private BlockingQueue<StoredProcedureCall> spcQueue;
	private FeatureExtractor featureExtractor;
	private TransactionFeaturesRecorder featureRecorder;
	private TransactionDependencyRecorder dependencyRecorder;

	// Components to simulate the scheduler
	private TPartStoredProcedureFactory factory;
	private BatchNodeInserter inserter;
	private TGraph graph;
	private boolean isBatching;
	
	public FeatureCollector(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, TGraph graph,
			boolean isBatching, TpartMetricWarehouse metricWarehouse) {
		
		// For generating execution plan and sp task
		this.factory = factory;
		this.inserter = inserter;
		this.graph = graph;
		this.isBatching = isBatching;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		// For collecting features
		featureExtractor = new FeatureExtractor(metricWarehouse);
		featureRecorder = new TransactionFeaturesRecorder();
		featureRecorder.startRecording();
		dependencyRecorder = new TransactionDependencyRecorder();
		dependencyRecorder.startRecording();
	}
	
	public void monitorTransaction(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall())
			spcQueue.add(spc);
	}

	@Override
	public void run() {
		List<TPartStoredProcedureTask> batchedTasks = 
				new ArrayList<TPartStoredProcedureTask>();
		
		Thread.currentThread().setName("feature-collector");
		
		while (true) {
			try {
				// Take a SP call
				StoredProcedureCall spc = spcQueue.take();
				
				// Queue to the batch
				TPartStoredProcedureTask task = convertToSpTask(spc);
				if (task.getProcedureType() == ProcedureType.NORMAL)
					batchedTasks.add(task);
				
				// Process the batch as TPartScheduler does
				if ((isBatching && batchedTasks.size() >= TPartScheduler.SCHEDULE_BATCH_SIZE)
						|| !isBatching) {
					processBatch(batchedTasks);
					batchedTasks.clear();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private TPartStoredProcedureTask convertToSpTask(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall()) {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(spc.getPid(), spc.getTxNum());
			sp.prepare(spc.getPars());
			return new TPartStoredProcedureTask(spc.getClientId(), spc.getConnectionId(),
					spc.getTxNum(), spc.getArrivedTime(), spc.getOu0StartTime(), sp);
		}
		
		return null;
	}
	
	private void processBatch(List<TPartStoredProcedureTask> batchedTasks) {
		// Insert the batch of tasks
		inserter.insertBatch(graph, batchedTasks);
		
		// add write back edges
		graph.addWriteBackEdge();
		
		// Record the features for each transaction
		for (TxNode node : graph.getTxNodes()) {
			TransactionFeatures features = featureExtractor.extractFeatures(node);
			featureRecorder.record(features);
			dependencyRecorder.record(features);
		}
		
		// Clean up the tx nodes
		graph.clear();
	}
}
