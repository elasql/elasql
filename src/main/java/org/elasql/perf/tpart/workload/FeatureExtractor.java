package org.elasql.perf.tpart.workload;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.hermes.FusionTGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

import org.elasql.storage.tx.concurrency.ConservativeOrderedLockMonitor;

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
		builder.addFeature("Number of Update Records", task.getUpdateSet().size());
		builder.addFeature("Number of Insert Records", task.getInsertSet().size());
		builder.addFeature("Number of Fully Replicated Records", extractFullyReplicatedCount(task.getReadSet()));
		
		builder.addFeature("Read Data Distribution", extractRecordDistribution(task.getReadSet(), graph));
		builder.addFeature("Read Data Distribution in Bytes", extractReadDistributionInBytes(task.getReadSet(), graph));
		builder.addFeature("Read Data in Cache Distribution", extractReadInCacheDistribution(task.getReadSet(), graph));
		builder.addFeature("Update Data Distribution", extractRecordDistribution(task.getUpdateSet(), graph));
		
		builder.addFeature("Number of Overflows in Fusion Table", getFusionTableOverflowCount(graph));

		builder.addFeature("Buffer Hit Rate", extractBufferHitRate());
		builder.addFeature("Avg Pin Count", extractBufferAvgPinCount());
		builder.addFeature("Pinned Buffer Count", extractPinnedBufferCount());
		
		builder.addFeature("Buffer RL Wait Count", extractBufferReadWaitCount());
		builder.addFeature("Buffer WL Wait Count", extractBufferWriteWaitCount());
		builder.addFeature("Block Lock Release Count", extractBlockLockReleaseCount());
		builder.addFeature("Block Lock Wait Count", extractBlockLockWaitCount());
		builder.addFeature("File Header Page Release Count", extractFhpReleaseCount());
		builder.addFeature("File Header Page Wait Count", extractFhpWaitCount());
		builder.addFeature("Page GetVal Wait Count", extractPageGetValWaitCount());
		builder.addFeature("Page SetVal Wait Count", extractPageSetValWaitCount());
		builder.addFeature("Page GetVal Release Count", extractPageGetValReleaseCount());
		builder.addFeature("Page SetVal Release Count", extractPageSetValReleaseCount());

		// Features below are from the servers
		builder.addFeature("System CPU Load", extractSystemCpuLoad());
		builder.addFeature("Process CPU Load", extractProcessCpuLoad());
		builder.addFeature("System Load Average", extractSystemLoadAverage());
		builder.addFeature("Thread Active Count", extractThreadActiveCount());
		
		// Features for i/o
		builder.addFeature("I/O Read Bytes", extractIOReadBytes());
		builder.addFeature("I/O Write Bytes", extractIOWriteBytes());
		builder.addFeature("I/O Queue Length", extractIOQueueLength());
		
		// Latch Features
		//builder.addFeature("xLock Latencies", extractxLockWaitTime());
		builder.addFeature("xLock SMA Latency", extractxLockWaitTimeSimpleMovingAverage());
		//builder.addFeature("xLock Anchor Counter", extractxLockAnchorCounter());
		//builder.addFeature("sLock Anchor Counter", extractsLockAnchorCounter());
		
		// Get dependencies
//		Set<Long> dependentTxs = dependencyAnalyzer.addAndGetDependency(
//				task.getTxNum(), task.getReadSet(), task.getWriteSet());
//		for (Long dependentTx : dependentTxs)
//			builder.addDependency(dependentTx);
		
		return builder.build();
	}
	
	private Double[] extractBufferHitRate() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Double[] bufferHitRates = new Double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			bufferHitRates[serverId] = metricWarehouse.getBufferHitRate(serverId);
		
		return bufferHitRates;
	}
	
	private Double[] extractBufferAvgPinCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Double[] bufferAvgPinCounts = new Double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			bufferAvgPinCounts[serverId] = metricWarehouse.getBufferAvgPinCount(serverId);
		
		return bufferAvgPinCounts;
	}
	
	private Integer[] extractPinnedBufferCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] pinnedBufferCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			pinnedBufferCounts[serverId] = metricWarehouse.getPinnedBufferCount(serverId);
		
		return pinnedBufferCounts;
	}
	
	private Integer[] extractBufferReadWaitCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] bufferWaitCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			bufferWaitCounts[serverId] = metricWarehouse.getBufferReadWaitCount(serverId);
		
		return bufferWaitCounts;
	}
	
	private Integer[] extractBufferWriteWaitCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] bufferWaitCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			bufferWaitCounts[serverId] = metricWarehouse.getBufferWriteWaitCount(serverId);
		
		return bufferWaitCounts;
	}
	
	private Integer[] extractBlockLockReleaseCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] blockWaitDiffs = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			blockWaitDiffs[serverId] = metricWarehouse.getBlockReleaseCount(serverId);
		
		return blockWaitDiffs;
	}
	
	private Integer[] extractBlockLockWaitCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] blockWaitCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			blockWaitCounts[serverId] = metricWarehouse.getBlockWaitCount(serverId);
		
		return blockWaitCounts;
	}
	
	private Integer[] extractFhpReleaseCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] fhpReleaseCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			fhpReleaseCounts[serverId] = metricWarehouse.getFhpReleaseCount(serverId);
		
		return fhpReleaseCounts;
	}
	
	private Integer[] extractFhpWaitCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] fhpWaitCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			fhpWaitCounts[serverId] = metricWarehouse.getFhpWaitCount(serverId);
		
		return fhpWaitCounts;
	}
	
	private Integer[] extractPageGetValWaitCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] pageGetValWaitCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			pageGetValWaitCounts[serverId] = metricWarehouse.getPageGetValWaitCount(serverId);
		
		return pageGetValWaitCounts;
	}
	
	private Integer[] extractPageSetValWaitCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] pageSetValWaitCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			pageSetValWaitCounts[serverId] = metricWarehouse.getPageSetValWaitCount(serverId);
		
		return pageSetValWaitCounts;
	}
	
	private Integer[] extractPageGetValReleaseCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] pageGetValReleaseCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			pageGetValReleaseCounts[serverId] = metricWarehouse.getPageGetValReleaseCount(serverId);
		
		return pageGetValReleaseCounts;
	}
	
	private Integer[] extractPageSetValReleaseCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] pageSetValReleaseCounts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			pageSetValReleaseCounts[serverId] = metricWarehouse.getPageSetValReleaseCount(serverId);
		
		return pageSetValReleaseCounts;
	}
	
	private Double[] extractSystemCpuLoad() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Double[] systemLoads = new Double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			systemLoads[serverId] = metricWarehouse.getSystemCpuLoad(serverId);
		
		return systemLoads;
	}
	
	private Double[] extractProcessCpuLoad() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Double[] processLoads = new Double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)	
			processLoads[serverId] = metricWarehouse.getProcessCpuLoad(serverId);
		
		return processLoads;
	}
	
	private Double[] extractSystemLoadAverage() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Double[] avgLoads = new Double[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			avgLoads[serverId] = metricWarehouse.getSystemLoadAverage(serverId);
		
		return avgLoads;
	}
	
	private Integer[] extractThreadActiveCount() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Integer[] counts = new Integer[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++)
			counts[serverId] = metricWarehouse.getThreadActiveCount(serverId);
		
		return counts;
	}
	
	private Integer[] extractRecordDistribution(Set<PrimaryKey> keys, TGraph graph) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (PrimaryKey key : keys) {
			// Skip fully replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			int partId = graph.getResourcePosition(key).getPartId();
			counts[partId]++;
		}
		
		Integer[] newCounts = new Integer[PartitionMetaMgr.NUM_PARTITIONS];
	    Arrays.setAll(newCounts, i -> counts[i]);
	    
		return newCounts;
	}
	
	private Integer[] extractReadDistributionInBytes(Set<PrimaryKey> keys, TGraph graph) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int[] size = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (PrimaryKey key : keys) {
			// Skip fully replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			int partId = graph.getResourcePosition(key).getPartId();
			size[partId]+= RecordSizeMaintainer.getRecordSize(key.getTableName());
		}
		
		Integer[] newSizes = new Integer[PartitionMetaMgr.NUM_PARTITIONS];
	    Arrays.setAll(newSizes, i -> size[i]);
	    
		return newSizes;
	}
	
	private Integer[] extractReadInCacheDistribution(Set<PrimaryKey> keys, TGraph graph) {
		int[] counts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		switch (Elasql.SERVICE_TYPE) {
		case HERMES:
		case LEAP:
		case HERMES_CONTROL:
			FusionTGraph fusionTGraph = (FusionTGraph) graph;
			for (PrimaryKey key : keys) {
				int partId = fusionTGraph.getCachedLocation(key);
				if (partId != -1)
					counts[partId]++;
			}
			break;
		default:
		}
		
		Integer[] newCounts = new Integer[PartitionMetaMgr.NUM_PARTITIONS];
	    Arrays.setAll(newCounts, i -> counts[i]);
	    
		return newCounts;
	}
	
	private int getFusionTableOverflowCount(TGraph graph) {
		switch (Elasql.SERVICE_TYPE) {
		case HERMES:
		case LEAP:
		case HERMES_CONTROL:
			FusionTGraph fusionTGraph = (FusionTGraph) graph;
			return fusionTGraph.getFusionTableOverflowCount();
		default:
			return 0;
		}
	}
	
	private int extractFullyReplicatedCount(Set<PrimaryKey> keys) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int count = 0;
		
		for (PrimaryKey key : keys) {
			// Skip fully replicated records
			if (partMgr.isFullyReplicated(key))
				count++;
		}
	    
		return count;
	}
	
	private Long[] extractIOReadBytes() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Long[] bytes = new Long[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			bytes[serverId] = metricWarehouse.getIOReadBytes(serverId);
		
		return bytes;
	}
	
	private Long[] extractIOWriteBytes() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Long[] bytes = new Long[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			bytes[serverId] = metricWarehouse.getIOWriteBytes(serverId);
		
		return bytes;
	}
	
	private Long[] extractIOQueueLength() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Long[] lengths = new Long[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			lengths[serverId] = metricWarehouse.getIOQueueLength(serverId);
		
		return lengths;
	}
	
	private Long[] extractxLockWaitTimeSimpleMovingAverage() {
		int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
		Long[] waitTimes = new Long[serverCount];
		
		for (int serverId = 0; serverId < serverCount; serverId++) 
			waitTimes[serverId] = metricWarehouse.getxLockSimpleMovingAverage(serverId);
		return waitTimes;
	}
	/*
	private int[] extractxLockAnchorCounter() {
		int[] xLockAnchor;
		xLockAnchor = metricWarehouse.getxLockAnchorCounter(0);
		return xLockAnchor;
	}
	
	private int[] extractsLockAnchorCounter() {
		int[] sLockAnchor;
		sLockAnchor = metricWarehouse.getsLockAnchorCounter(0);
		return sLockAnchor;
	}
	*/
}
