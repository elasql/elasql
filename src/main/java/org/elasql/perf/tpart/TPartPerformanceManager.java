package org.elasql.perf.tpart;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.FeatureExtractor;
import org.elasql.perf.tpart.ai.TransactionDependencyRecorder;
import org.elasql.perf.tpart.ai.TransactionFeatures;
import org.elasql.perf.tpart.ai.TransactionFeaturesRecorder;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;

public class TPartPerformanceManager extends PerformanceManager {

	private TPartStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue;
	
	// For cost estimation
	private FeatureExtractor featureExtractor;
	private TransactionFeaturesRecorder featureRecorder;
	private TransactionDependencyRecorder dependencyRecorder;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory) {
		this.factory = factory;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		if (Estimator.ENABLE_COLLECTING_DATA) {
			featureExtractor = new FeatureExtractor();
			featureRecorder = new TransactionFeaturesRecorder();
			featureRecorder.startRecording();
			dependencyRecorder = new TransactionDependencyRecorder();
			dependencyRecorder.startRecording();
		}
	}

	@Override
	public void run() {
		Thread.currentThread().setName("TPart Performance Manager");
		
		while (true) {
			try {
				StoredProcedureCall spc = spcQueue.take();
				
				// Convert the call to a task
				TPartStoredProcedureTask task = convertToSpTask(spc);
				
				if (Estimator.ENABLE_COLLECTING_DATA) {
					TransactionFeatures features = featureExtractor.extractFeatures(task);
					featureRecorder.record(features);
					dependencyRecorder.record(features);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		spcQueue.add(spc);
	}
	
	private TPartStoredProcedureTask convertToSpTask(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall()) {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(spc.getPid(), spc.getTxNum());
			sp.prepare(spc.getPars());
			return new TPartStoredProcedureTask(spc.getClientId(), spc.getConnectionId(),
					spc.getTxNum(), spc.getArrivedTime(), sp);
		}
		
		return null;
	}
}
