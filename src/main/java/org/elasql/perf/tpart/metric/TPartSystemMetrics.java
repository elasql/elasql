package org.elasql.perf.tpart.metric;

import org.elasql.perf.MetricReport;

public class TPartSystemMetrics implements MetricReport {
	
	private static final long serialVersionUID = 20210831001L;

	public static class Builder {
		
		private int serverId;
		
		// XXX: for demo
		private int fakeMetric;
		private int threadPoolSize;
		
		public Builder(int serverId) {
			this.serverId = serverId;
		}
		
		// XXX: for demo
		public void setFakeMetric(int fake) {
			this.fakeMetric = fake;
		}
		public void setThreadPoolSize(int threadPoolSize) {
			this.threadPoolSize = threadPoolSize;
		}

		public TPartSystemMetrics build() {
			TPartSystemMetrics metrics = new TPartSystemMetrics();
			metrics.serverId = serverId;
			metrics.fakeMetric = fakeMetric;
			metrics.threadPoolSize = threadPoolSize;
			return metrics;
		}
	}
	
	private int serverId;
	
	// XXX: for demo
	private int fakeMetric;
	private int threadPoolSize;
	
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
	
	public int getThreadPoolSize() {
		return threadPoolSize;
	}

}
