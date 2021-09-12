package org.elasql.perf.tpart.workload;

import java.util.Arrays;
import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TxNode;
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
	
	public TransactionFeatures extractFeatures(TxNode txNode) {
		// Check if transaction requests are given in the total order
		if (txNode.getTxNum() <= lastProcessedTxNum)
			throw new RuntimeException(String.format(
					"Transaction requests are not passed to FeatureExtractor "
					+ "in the total order: %d, last processed tx: %d",
					txNode.getTxNum(), lastProcessedTxNum));
		
		// Get the task
		TPartStoredProcedureTask task = txNode.getTask();
		
		// Extract the features
		TransactionFeatures.Builder builder = new TransactionFeatures.Builder(task.getTxNum());
		
		// Get features (all features in TransactionFeatures.FEATURE_KEYS must be set)
		builder.addFeature("Start Time", task.getArrivedTime());
		builder.addFeature("Number of Read Records", task.getReadSet().size());
		builder.addFeature("Number of Write Records", task.getWriteSet().size());
		builder.addFeature("Number of Cache Writes per Server", extractCacheWrites(txNode));
		builder.addFeature("Number of Storage Writes per Server", extractStorageWrites(txNode));

		// Features below are from the servers
		extractSystemCpuLoad(builder);
		extractProcessCpuLoad(builder);
		extractSystemLoadAverage(builder);
		extractThreadActiveCount(builder);
		
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
	
	private String extractCacheWrites(TxNode node) {
		PartitionMetaMgr partMeta = Elasql.partitionMetaMgr();
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		// Write: pass to the next transactions
		for (Edge writeEdge : node.getWriteEdges()) {
			int partId = writeEdge.getTarget().getPartId();
			counts[partId]++;
		}
		
		// Writeback: save on the local cache or storage
		for (Edge writeEdge : node.getWriteBackEdges()) {
			PrimaryKey key = writeEdge.getResourceKey();
			int partId = writeEdge.getTarget().getPartId();
			if (!partMeta.isFullyReplicated(key) && partMeta.getPartition(key) != partId)
				counts[partId]++;
		}
		
		return Arrays.toString(counts);
	}
	
	private String extractStorageWrites(TxNode node) {
		PartitionMetaMgr partMeta = Elasql.partitionMetaMgr();
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		// Only writeback write to storage
		for (Edge writeEdge : node.getWriteBackEdges()) {
			PrimaryKey key = writeEdge.getResourceKey();
			int partId = writeEdge.getTarget().getPartId();
			if (partMeta.isFullyReplicated(key) || partMeta.getPartition(key) == partId)
				counts[partId]++;
		}
		
		return Arrays.toString(counts);
	}
}
