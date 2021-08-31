package org.elasql.perf.tpart.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.task.Task;

public class MetricWarehouse extends Task {

	private BlockingQueue<TPartSystemMetrics> metricQueue;
	
	// XXX: for demo
	private Map<Integer, Integer> fakeMetrics;
	
	public MetricWarehouse() {
		this.metricQueue = new LinkedBlockingQueue<TPartSystemMetrics>();
		this.fakeMetrics = new HashMap<Integer, Integer>();
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
}
