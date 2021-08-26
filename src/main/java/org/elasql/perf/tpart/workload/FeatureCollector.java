package org.elasql.perf.tpart.workload;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.core.server.task.Task;

/**
 * A collector that collects the features of transactions.
 * 
 * @author Yu-Shan Lin
 */
public class FeatureCollector extends Task {

	private TPartStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue;
	private FeatureExtractor featureExtractor;
	private TransactionFeaturesRecorder featureRecorder;
	private TransactionDependencyRecorder dependencyRecorder;
	
	public FeatureCollector(TPartStoredProcedureFactory factory) {
		this.factory = factory;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		featureExtractor = new FeatureExtractor();
		featureRecorder = new TransactionFeaturesRecorder();
		featureRecorder.startRecording();
		dependencyRecorder = new TransactionDependencyRecorder();
		dependencyRecorder.startRecording();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("feature-collector");
		
		while (true) {
			try {
				StoredProcedureCall spc = spcQueue.take();
				TPartStoredProcedureTask task = convertToSpTask(spc);
				TransactionFeatures features = featureExtractor.extractFeatures(task);
				featureRecorder.record(features);
				dependencyRecorder.record(features);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
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
