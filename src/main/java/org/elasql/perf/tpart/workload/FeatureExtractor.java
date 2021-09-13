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
		
		return quoteString(Arrays.toString(counts));
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
		
		return quoteString(Arrays.toString(counts));
	}
}
