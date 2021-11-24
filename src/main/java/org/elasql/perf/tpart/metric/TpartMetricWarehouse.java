package org.elasql.perf.tpart.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.MetricWarehouse;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.server.task.Task;

import oshi.hardware.CentralProcessor.TickType;

public class TpartMetricWarehouse extends Task implements MetricWarehouse {
	
	private static class StampedMetric {
		long timestamp;
		TPartSystemMetrics metric;
		
		StampedMetric(long timestamp, TPartSystemMetrics metric) {
			this.timestamp = timestamp;
			this.metric = metric;
		}
	}
	
	private BlockingQueue<TPartSystemMetrics> metricQueue;
	
	// Metrics
	private Map<Integer, List<StampedMetric>> metricStore;
	
	public TpartMetricWarehouse() {
		metricQueue = new LinkedBlockingQueue<TPartSystemMetrics>();
		metricStore = new HashMap<Integer, List<StampedMetric>>();
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			metricStore.put(nodeId, new ArrayList<StampedMetric>());
		}
	}
	
	public void receiveMetricReport(TPartSystemMetrics metrics) {
		metricQueue.add(metrics);
		
		// Debug: print some records
//		if (metrics.getServerId() == 0) {
//			System.out.println("System CPU Usage: " + getSystemCpuLoad(0));
//			System.out.println("System CPU Usage (last 5 secs): " + getAveragedSystemCpuLoad(0, 5000));
//			System.out.println("Process CPU Usage: " + getProcessCpuLoad(0));
//			System.out.println("Load average: " + getSystemLoadAverage(0));
//		}
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
		metricStore.get(metrics.getServerId()).add(
				new StampedMetric(timestamp, metrics));
		
		// debug code
//		System.out.println(String.format("Receives a metric report from server %d with CPU load: %f",
//				metrics.getServerId(), metrics.getSystemCpuLoad()));
	}
	
	public synchronized double getBufferHitRate(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric.getBufferHitRate();
		}
	}
	
	public synchronized double getBufferAvgPinCount(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric.getBufferAvgPinCount();
		}
	}
	
	public synchronized int getPinnedBufferCount(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getPinnedBufferCount();
		}
	}
	
	public synchronized int getBufferReadWaitCount(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getBufferReadWaitCount();
		}
	}
	
	public synchronized int getBufferWriteWaitCount(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getBufferWriteWaitCount();
		}
	}
	
	public synchronized double getProcessCpuLoad(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.size() < 2) {
			return 0.0;
		} else {
			TPartSystemMetrics lastRec = history.get(history.size() - 1).metric;
			TPartSystemMetrics secLastRec = history.get(history.size() - 2).metric;
			
			double processWorkingTime = 
					(lastRec.getProcessUserTime() - secLastRec.getProcessUserTime() +
					 lastRec.getProcessKernelTime() - secLastRec.getProcessKernelTime());
			double processUpTime = (lastRec.getProcessUpTime() - 
					secLastRec.getProcessUpTime());
					
			return processWorkingTime / processUpTime;
		}
	}
	
	public synchronized double getSystemCpuLoad(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.size() < 2) {
			return 0.0;
		} else {
			long[] endTicks = history.get(history.size() - 1)
					.metric.getSystemCpuLoadTicks();
			long[] startTicks = null;
			
			// Sampling too fast may get the same value between last
			// two records, so we need to find the last two records
			// with different values.
			for (int idx = history.size() - 2; idx >= 0; idx--) {
				startTicks = history.get(idx)
						.metric.getSystemCpuLoadTicks();
				if (!Arrays.equals(startTicks, endTicks)) {
					break;
				}
			}
			
			return getCpuUsageBetween(startTicks, endTicks);
		}
	}
	
	public synchronized double getAveragedSystemCpuLoad(int serverId, long timeLength) {
		List<StampedMetric> history = metricStore.get(serverId);
		
		if (history.size() < 2) {
			return 0.0;
		}
		
		// Get the latest CPU load ticks
		long[] endTicks = history.get(history.size() - 1).metric.getSystemCpuLoadTicks();
		
		// Get the ticks at the beginning of the time window
		long[] startTicks = null;
		long startTime = System.currentTimeMillis() - timeLength;
		for (int idx = history.size() - 2; idx >= 0; idx--) {
			StampedMetric record = history.get(idx);
			startTicks = record.metric.getSystemCpuLoadTicks();
			if (record.timestamp < startTime)
				break;
		}
		
		return getCpuUsageBetween(startTicks, endTicks);
	}
	
	public synchronized double getSystemLoadAverage(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0.0;
		} else {
			return history.get(history.size() - 1).metric.getSystemLoadAverage();
		}
	}
	
	public synchronized int getThreadActiveCount(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getThreadActiveCount();
		}
	}
	
	public synchronized long getIOReadBytes(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getIOReadBytes();
		}
	}
	
	public synchronized long getIOWriteBytes(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getIOWriteBytes();
		}
	}
	
	public synchronized long getIOQueueLength(int serverId) {
		List<StampedMetric> history = metricStore.get(serverId);
		if (history.isEmpty()) {
			return 0;
		} else {
			return history.get(history.size() - 1).metric.getIOQueueLength();
		}
	}
	
	private double getCpuUsageBetween(long[] startTicks, long[] endTicks) {
		long total = 0;
        for (int i = 0; i < startTicks.length; i++) {
            total += endTicks[i] - startTicks[i];
        }
        // CPU Usage = 1 - (idle + IOWait) / total
        long idle = endTicks[TickType.IDLE.getIndex()] + endTicks[TickType.IOWAIT.getIndex()]
                - startTicks[TickType.IDLE.getIndex()] - startTicks[TickType.IOWAIT.getIndex()];
        return total > 0 && idle >= 0 ? (double) (total - idle) / total : 0d;
	}
}
