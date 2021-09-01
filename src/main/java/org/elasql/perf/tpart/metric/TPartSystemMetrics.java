package org.elasql.perf.tpart.metric;

import org.elasql.perf.MetricReport;

public class TPartSystemMetrics implements MetricReport {
	
	private static final long serialVersionUID = 20210831001L;

	public static class Builder {
		
		private int serverId;
		
		// XXX: for demo
		private int fakeMetric;
		
		private double systemCpuLoad;
		private double processCpuLoad;
		private double systemLoadAverage;
		
		private int threadActiveCount;
		
		public Builder(int serverId) {
			this.serverId = serverId;
		}
		
		// XXX: for demo
		public void setFakeMetric(int fake) {
			this.fakeMetric = fake;
		}
		
		public void setSystemCpuLoad(double systemCpuLoad) {
			this.systemCpuLoad = systemCpuLoad;
		}
		
		public void setProcessCpuLoad(double processCpuLoad) {
			this.processCpuLoad = processCpuLoad;
		}
		
		public void setSystemLoadAverage(double systemLoadAverage) {
			this.systemLoadAverage = systemLoadAverage;
		}
		
		public void setThreadActiveCount(int threadActiveCount) {
			this.threadActiveCount = threadActiveCount;
		}
		
		public TPartSystemMetrics build() {
			TPartSystemMetrics metrics = new TPartSystemMetrics();
			metrics.serverId = serverId;
			
			metrics.fakeMetric = fakeMetric;
			
			metrics.systemCpuLoad = systemCpuLoad;
			metrics.processCpuLoad = processCpuLoad;
			metrics.systemLoadAverage = systemLoadAverage;
			
			metrics.threadActiveCount = threadActiveCount;
			return metrics;
		}
	}
	
	private int serverId;
	
	// XXX: for demo
	private int fakeMetric;
	
	private double systemCpuLoad;
	private double processCpuLoad;
	private double systemLoadAverage;
	
	private int threadActiveCount;
	
	private TPartSystemMetrics() {
		// do nothing
	}
	
	public int getServerId() {
		return serverId;
	}
	
	// XXX: for demo
	public int getFakeMetric() {
		return fakeMetric;
	}
	
	public double getSystemCpuLoad() {
		return systemCpuLoad;
	}
	
	public double getProcessCpuLoad() {
		return processCpuLoad;
	}
	
	public double getSystemLoadAverage() {
		return systemLoadAverage;
	}
	
	public int getThreadActiveCount() {
		return threadActiveCount;
	}
}
