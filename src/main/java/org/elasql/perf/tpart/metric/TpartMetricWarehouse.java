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
	private Map<Integer, Integer> threadPoolSizes;
	
	public TpartMetricWarehouse() {
		this.metricQueue = new LinkedBlockingQueue<TPartSystemMetrics>();
		this.fakeMetrics = new HashMap<Integer, Integer>();
		this.threadPoolSizes = new HashMap<Integer, Integer>();
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
		System.out.println(String.format("Receives a report from server %d with metric: %d",
				metrics.getServerId(), metrics.getFakeMetric()));
	}
	
	// XXX: for demo
	public synchronized Integer getFakeMetric(int serverId) {
		return fakeMetrics.get(serverId);
	}
	public synchronized Integer getThreadPoolSize(int serverId) {
		return threadPoolSizes.get(serverId);
	}
}
