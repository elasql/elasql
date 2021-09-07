package org.elasql.perf.tpart.workload;

import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.storage.metadata.PartitionMetaMgr;

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

		// Features below are from the servers
		extractSystemCpuLoad(builder);
		extractProcessCpuLoad(builder);
		extractSystemLoadAverage(builder);
		extractThreadActiveCount(builder);
		extractThreadPoolSize(builder);
		
		// Get dependencies
		Set<Long> dependentTxs = dependencyAnalyzer.addAndGetDependency(
				task.getTxNum(), task.getReadSet(), task.getWriteSet());
		for (Long dependentTx : dependentTxs)
			builder.addDependency(dependentTx);
		
		return builder.build();
	}
	
	private void extractSystemCpuLoad(TransactionFeatures.Builder builder) {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			builder.addFeatureWithServerId(
					"System CPU Load",
					metricWarehouse.getSystemCpuLoad(serverId),
					serverId
				);
		}
	}
	
	private void extractProcessCpuLoad(TransactionFeatures.Builder builder) {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			builder.addFeatureWithServerId(
					"Process CPU Load",
					metricWarehouse.getProcessCpuLoad(serverId),
					serverId
				);
		}
	}
	
	private void extractSystemLoadAverage(TransactionFeatures.Builder builder) {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			builder.addFeatureWithServerId(
					"System Load Average",
					metricWarehouse.getSystemLoadAverage(serverId),
					serverId
				);
		}
	}
	
	private void extractThreadActiveCount(TransactionFeatures.Builder builder) {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			builder.addFeatureWithServerId(
					"Thread Active Count",
					metricWarehouse.getThreadActiveCount(serverId),
					serverId
				);
		}
	}
	private void extractThreadPoolSize(TransactionFeatures.Builder builder) {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		
		for (int serverId = 0; serverId < serverCount; serverId++) {
			builder.addFeatureWithServerId(
					"Thread Pool Size",
					metricWarehouse.getThreadPoolSize(serverId),
					serverId
				);
		}
	}
}
