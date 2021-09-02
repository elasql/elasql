package org.elasql.perf.tpart.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.MetricWarehouse;
import org.vanilladb.core.server.task.Task;

public class TpartMetricWarehouse extends Task implements MetricWarehouse {

	private BlockingQueue<TPartSystemMetrics> metricQueue;
	
	// XXX: for demo
	private Map<Integer, Integer> fakeMetrics;
	private Map<Integer, Double> processCpuLoad;
	private Map<Integer, Double> systemCpuLoad;
	private Map<Integer, Double> systemLoadAverage;
	
	private Map<Integer, Integer> threadActiveCount;
	
	public TpartMetricWarehouse() {
		this.metricQueue = new LinkedBlockingQueue<TPartSystemMetrics>();
		this.fakeMetrics = new HashMap<Integer, Integer>();
		
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
		// XXX: for demo
		fakeMetrics.put(metrics.getServerId(), metrics.getFakeMetric());
		
		processCpuLoad.put(metrics.getServerId(), metrics.getProcessCpuLoad());
		systemCpuLoad.put(metrics.getServerId(), metrics.getSystemCpuLoad());
		systemLoadAverage.put(metrics.getServerId(), metrics.getSystemLoadAverage());
		
		threadActiveCount.put(metrics.getServerId(),  metrics.getThreadActiveCount());
		
		// debug code
		// System.out.println(String.format("Receives a report from server %d with average system load: %f",
		//	metrics.getServerId(), metrics.getSystemLoadAverage()));
	}
	
	// XXX: for demo
	public synchronized Integer getFakeMetric(int serverId) {
		return fakeMetrics.get(serverId);
	}
	
	public synchronized Double getProcessCpuLoad(int serverId) {
		return processCpuLoad.get(serverId);
	}
	
	public synchronized Double getSystemCpuLoad(int serverId) {
		return systemCpuLoad.get(serverId);
	}
	
	public synchronized Double getSystemLoadAverage(int serverId) {
		return systemLoadAverage.get(serverId);
	}
	
	public synchronized Integer getThreadActiveCount(int serverId) {
		return threadActiveCount.get(serverId);
	}
}
