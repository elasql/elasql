package org.elasql.perf.tpart.metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.MetricWarehouse;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.server.task.Task;

public class TpartMetricWarehouse extends Task implements MetricWarehouse {
	
	private static class StampedMetric<M> {
		long timestamp;
		M metric;
		
		StampedMetric(long timestamp, M metric) {
			this.timestamp = timestamp;
			this.metric = metric;
		}
	}
	
	private BlockingQueue<TPartSystemMetrics> metricQueue;

	// Buffer metrics
	private Map<Integer, List<StampedMetric<Double>>> bufferHitRate;
	private Map<Integer, List<StampedMetric<Double>>> bufferAvgPinCount;
	private Map<Integer, List<StampedMetric<Integer>>> pinnedBufferCount;
	
	// Load metrics
	private Map<Integer, List<StampedMetric<Double>>> systemCpuLoad;
	private Map<Integer, List<StampedMetric<Double>>> processCpuLoad;
	private Map<Integer, List<StampedMetric<Double>>> systemLoadAverage;
	
	// Thread metrics
	private Map<Integer, List<StampedMetric<Integer>>> threadActiveCount;
	
	public TpartMetricWarehouse() {
		metricQueue = new LinkedBlockingQueue<TPartSystemMetrics>();
		
		bufferHitRate = new HashMap<Integer, List<StampedMetric<Double>>>();
		bufferAvgPinCount = new HashMap<Integer, List<StampedMetric<Double>>>();
		pinnedBufferCount = new HashMap<Integer, List<StampedMetric<Integer>>>();
		processCpuLoad = new HashMap<Integer, List<StampedMetric<Double>>>();
		systemCpuLoad = new HashMap<Integer, List<StampedMetric<Double>>>();
		systemLoadAverage = new HashMap<Integer, List<StampedMetric<Double>>>();
		threadActiveCount = new HashMap<Integer, List<StampedMetric<Integer>>>();
		
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			bufferHitRate.put(nodeId, new ArrayList<StampedMetric<Double>>());
			bufferAvgPinCount.put(nodeId, new ArrayList<StampedMetric<Double>>());
			pinnedBufferCount.put(nodeId, new ArrayList<StampedMetric<Integer>>());
			processCpuLoad.put(nodeId, new ArrayList<StampedMetric<Double>>());
			systemCpuLoad.put(nodeId, new ArrayList<StampedMetric<Double>>());
			systemLoadAverage.put(nodeId, new ArrayList<StampedMetric<Double>>());
			threadActiveCount.put(nodeId, new ArrayList<StampedMetric<Integer>>());
		}
	}
	
	public void receiveMetricReport(TPartSystemMetrics metrics) {
		metricQueue.add(metrics);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("metric-warehouse");
		
		while (true) {
			try {
				TPartSystemMetrics metrics = metricQueue.take();
				recordMetric(metrics);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private synchronized void recordMetric(TPartSystemMetrics metrics) {
		long timestamp = System.currentTimeMillis();
		
		bufferHitRate.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getBufferHitRate()));
		bufferAvgPinCount.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getBufferAvgPinCount()));
		pinnedBufferCount.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getPinnedBufferCount()));
		processCpuLoad.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getProcessCpuLoad()));
		systemCpuLoad.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getSystemCpuLoad()));
		systemLoadAverage.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getSystemLoadAverage()));
		threadActiveCount.get(metrics.getServerId()).add(
				new StampedMetric<>(timestamp, metrics.getThreadActiveCount()));
		
		// debug code
//		System.out.println(String.format("Receives a metric report from server %d with CPU load: %f",
//				metrics.getServerId(), metrics.getSystemCpuLoad()));
	}
	
	public synchronized double getBufferHitRate(int serverId) {
		List<StampedMetric<Double>> history = bufferHitRate.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
	
	public synchronized double getBufferAvgPinCount(int serverId) {
		List<StampedMetric<Double>> history = bufferAvgPinCount.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
	
	public synchronized int getPinnedBufferCount(int serverId) {
		List<StampedMetric<Integer>> history = pinnedBufferCount.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
	
	public synchronized double getProcessCpuLoad(int serverId) {
		List<StampedMetric<Double>> history = processCpuLoad.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
	
	public synchronized double getSystemCpuLoad(int serverId) {
		List<StampedMetric<Double>> history = systemCpuLoad.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
	
	public synchronized double getAveragedSystemCpuLoad(int serverId, long timeLength) {
		long startTime = System.currentTimeMillis() - timeLength;
		double sum = 0.0;
		int count = 0;

		List<StampedMetric<Double>> history = systemCpuLoad.get(serverId);
		for (int idx = history.size() - 1; idx >= 0; idx--) {
			StampedMetric<Double> metric = history.get(idx);
			if (metric.timestamp < startTime)
				break;
			sum += metric.metric;
			count++;
		}
		
		// Special case: no data in the window
		if (count == 0) {
			if (history.isEmpty()) {
				return 0.0;
			} else {
				return history.get(history.size() - 1).metric;
			}
		}
		
		return sum / count;
	}
	
	public synchronized double getSystemLoadAverage(int serverId) {
		List<StampedMetric<Double>> history = systemLoadAverage.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
	
	public synchronized int getThreadActiveCount(int serverId) {
		List<StampedMetric<Integer>> history = threadActiveCount.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric;
		}
	}
}
