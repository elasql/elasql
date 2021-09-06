package org.elasql.perf.tpart.workload;

import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;

/**
 * A processor to extract features from a transaction request. The transaction
 * must be given in the total order. 
 * 
 * @author Yu-Shan Lin, Yu-Xuan Lin
 */
public class FeatureExtractor {
	
	private long lastProcessedTxNum = -1;
	
	private TransactionDependencyAnalyzer dependencyAnalyzer =
			new TransactionDependencyAnalyzer();
	
	private TpartMetricWarehouse metricWarehouse;
	
	public FeatureExtractor(TpartMetricWarehouse metricWarehouse) {
		this.metricWarehouse = metricWarehouse;
	}
	
	public TransactionFeatures extractFeatures(TPartStoredProcedureTask task) {
		// Check if transaction requests are given in the total order
		if (task.getTxNum() <= lastProcessedTxNum)
			throw new RuntimeException(String.format(
					"Transaction requests are not passed to FeatureExtractor "
					+ "in the total order: %d, last processed tx: %d",
					task.getTxNum(), lastProcessedTxNum));
			
		// Extract the features
		TransactionFeatures.Builder builder = new TransactionFeatures.Builder(task.getTxNum());
		
		// Get features (all features in TransactionFeatures.FEATURE_KEYS must be set)
		builder.addFeature("Start Time", task.getArrivedTime());
		builder.addFeature("Number of Read Records", task.getReadSet().size());
		builder.addFeature("Number of Write Records", task.getWriteSet().size());
		
		// Some transaction features come from the Matrix WareHouse
		builder.addFeatures("Thread Pool Size", metricWarehouse.getThreadPoolSize());
		
		// Get dependencies
		Set<Long> dependentTxs = dependencyAnalyzer.addAndGetDependency(
				task.getTxNum(), task.getReadSet(), task.getWriteSet());
		for (Long dependentTx : dependentTxs)
			builder.addDependency(dependentTx);
		
		return builder.build();
	}
}
