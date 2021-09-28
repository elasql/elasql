package org.elasql.perf.tpart.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.MetricWarehouse;
import org.vanilladb.core.server.task.Task;

public class TpartMetricWarehouse extends Task implements MetricWarehouse {

	private BlockingQueue<TPartSystemMetrics> metricQueue;
	
	private Map<Integer, Double> systemCpuLoad;
	private Map<Integer, Double> processCpuLoad;
	private Map<Integer, Double> systemLoadAverage;
	
	private Map<Integer, Integer> threadActiveCount;
	
	public TpartMetricWarehouse() {
		this.metricQueue = new LinkedBlockingQueue<TPartSystemMetrics>();
		
		this.processCpuLoad = new HashMap<Integer, Double>();
		this.systemCpuLoad = new HashMap<Integer, Double>();
		this.systemLoadAverage = new HashMap<Integer, Double>();
		
		this.threadActiveCount = new HashMap<Integer, Integer>();
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
		processCpuLoad.put(metrics.getServerId(), metrics.getProcessCpuLoad());
		systemCpuLoad.put(metrics.getServerId(), metrics.getSystemCpuLoad());
		systemLoadAverage.put(metrics.getServerId(), metrics.getSystemLoadAverage());
		
		threadActiveCount.put(metrics.getServerId(),  metrics.getThreadActiveCount());
		
		// debug code
		// System.out.println(String.format("Receives a report from server %d with average system load: %f",
		//  metrics.getServerId(), metrics.getSystemLoadAverage()));
	}
	
	public synchronized double getProcessCpuLoad(int serverId) {
		Double load = processCpuLoad.get(serverId);
		if (load == null)
			return 0.0;
		else
			return load;
	}
	
	public synchronized double getSystemCpuLoad(int serverId) {
		Double load = systemCpuLoad.get(serverId);
		if (load == null)
			return 0.0;
		else
		return load;
	}
	
	public synchronized double getSystemLoadAverage(int serverId) {
		Double load = systemLoadAverage.get(serverId);
		if (load == null)
			return 0.0;
		else
			return load;
	}
	
	public synchronized int getThreadActiveCount(int serverId) {
		Integer count = threadActiveCount.get(serverId);
		if (count == null)
			return 0;
		else
			return count;
	}
}
