package org.elasql.perf.tpart.workload;

import java.util.Arrays;
import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
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
		builder.addFeature("Number of Write per Partition", extractWriteOriginalParts(task.getWriteSet()));

		// Features below are from the servers
		builder.addFeature("System CPU Load", extractSystemCpuLoad());
		builder.addFeature("Process CPU Load", extractProcessCpuLoad());
		builder.addFeature("System Load Average", extractSystemLoadAverage());
		builder.addFeature("Thread Active Count", extractThreadActiveCount());
		
		// Get dependencies
		Set<Long> dependentTxs = dependencyAnalyzer.addAndGetDependency(
				task.getTxNum(), task.getReadSet(), task.getWriteSet());
		for (Long dependentTx : dependentTxs)
			builder.addDependency(dependentTx);
		
		return builder.build();
	}
	
	private String quoteString(String str)  {
		return "\"" + str + "\"";
	}
	
	private String extractSystemCpuLoad() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		double[] systemLoads = new double[serverCount];
		for (int serverId = 0; serverId < serverCount; serverId++)	
			systemLoads[serverId] = metricWarehouse.getSystemCpuLoad(serverId);
		return quoteString(Arrays.toString(systemLoads));
	}
	
	private String extractProcessCpuLoad() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		double[] processLoads = new double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			processLoads[serverId] = metricWarehouse.getProcessCpuLoad(serverId);
		
		return quoteString(Arrays.toString(processLoads));
	}
	
	private String extractSystemLoadAverage() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		double[] avgLoads = new double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			avgLoads[serverId] = metricWarehouse.getSystemLoadAverage(serverId);
		
		return quoteString(Arrays.toString(avgLoads));
	}
	
	private String extractThreadActiveCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		int[] counts = new int[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)
			counts[serverId] = metricWarehouse.getThreadActiveCount(serverId);
		
		return quoteString(Arrays.toString(counts));
	}
	
	private String extractWriteOriginalParts(Set<PrimaryKey> writeKeys) {
		PartitionMetaMgr partMeta = Elasql.partitionMetaMgr();
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (PrimaryKey writeKey : writeKeys) {
			int partId = partMeta.getPartition(writeKey);
			counts[partId]++;
		}
		
		return quoteString(Arrays.toString(counts));
	}
}
