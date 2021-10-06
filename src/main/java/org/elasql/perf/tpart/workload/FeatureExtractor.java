package org.elasql.perf.tpart.workload;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
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
	private static DecimalFormat formatter = new DecimalFormat("#.######");
	
	private TransactionDependencyAnalyzer dependencyAnalyzer =
			new TransactionDependencyAnalyzer();
	
	private TpartMetricWarehouse metricWarehouse;
	
	public FeatureExtractor(TpartMetricWarehouse metricWarehouse) {
		this.metricWarehouse = metricWarehouse;
	}
	
	/**
	 * Extracts the features from the stored procedure and the current T-grpah.
	 * Note that if stored procedures are processed in batches, some stored
	 * procedures in front of the current one may not yet be inserted to the
	 * T-graph.
	 * 
	 * @param task the analyzed task of the stored procedure
	 * @param graph the latest T-graph
	 * @return the features of the stored procedure for cost estimation
	 */
	public TransactionFeatures extractFeatures(TPartStoredProcedureTask task, TGraph graph) {
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
		builder.addFeature("Read Data Distribution", extractReadDistribution(task.getReadSet(), graph));

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
		String[] systemLoads = new String[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			systemLoads[serverId] = formatter.format(metricWarehouse.getSystemCpuLoad(serverId));
		
		return quoteString(Arrays.toString(systemLoads));
	}
	
	private String extractProcessCpuLoad() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		String[] processLoads = new String[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			processLoads[serverId] = formatter.format(metricWarehouse.getProcessCpuLoad(serverId));
		
		return quoteString(Arrays.toString(processLoads));
	}
	
	private String extractSystemLoadAverage() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		String[] avgLoads = new String[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			avgLoads[serverId] = formatter.format(metricWarehouse.getSystemLoadAverage(serverId));
		
		return quoteString(Arrays.toString(avgLoads));
	}
	
	private String extractThreadActiveCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		int[] counts = new int[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)
			counts[serverId] = metricWarehouse.getThreadActiveCount(serverId);
		
		return quoteString(Arrays.toString(counts));
	}
	
	private String extractReadDistribution(Set<PrimaryKey> readKeys, TGraph graph) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (PrimaryKey readKey : readKeys) {
			// Skip fully replicated records
			if (partMgr.isFullyReplicated(readKey))
				continue;
			
			int partId = graph.getResourcePosition(readKey).getPartId();
			counts[partId]++;
		}
		
		return quoteString(Arrays.toString(counts));
	}
}
