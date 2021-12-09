package org.elasql.perf.tpart.metric;

import org.elasql.perf.MetricReport;

public class TPartSystemMetrics implements MetricReport {
	
	private static final long serialVersionUID = 20210831001L;

	public static class Builder {
		private int serverId;
		
		// Buffers
		private double bufferHitRate;
		private double bufferAvgPinCount;
		private int pinnedBufferCount;
		
		private int bufferReadWaitCount;
		private int bufferWriteWaitCount;
		private int blockReleaseCount;
		private int blockWaitCount;
		private int fhpReleaseCount;
		private int fhpWaitCount;
		private int pageGetValWaitCount;
		private int pageSetValWaitCount;
		private int pageGetValReleaseCount;
		private int pageSetValReleaseCount;
		
		// CPU Usage and Load
		private long[] systemCpuLoadTicks;
		private double systemLoadAverage;
		private long processUserTime;
		private long processKernelTime;
		private long processUpTime;
		
		// Threads
		private int threadActiveCount;
		
		// I/O
		private long ioReadBytes;
		private long ioWriteBytes;
		private long ioQueueLength;
		
		public Builder(int serverId) {
			this.serverId = serverId;
		}
		
		public void setBufferHitRate(double bufferHitRate) {
			this.bufferHitRate = bufferHitRate;
		}
		
		public void setBufferAvgPinCount(double avgPinCount) {
			this.bufferAvgPinCount = avgPinCount;
		}
		
		public void setPinnedBufferCount(int pinnedBufferCount) {
			this.pinnedBufferCount = pinnedBufferCount;
		}
		
		public void setBufferReadWaitCount(int bufferReadWaitCount) {
			this.bufferReadWaitCount = bufferReadWaitCount;
		}
		
		public void setBufferWriteWaitCount(int bufferWriteWaitCount) {
			this.bufferWriteWaitCount = bufferWriteWaitCount;
		}
		
		public void setBlockReleaseCount(int blockReleaseCount) {
			this.blockReleaseCount = blockReleaseCount;
		}
		
		public void setBlockWaitCount(int blockWaitCount) {
			this.blockWaitCount = blockWaitCount;
		}
		
		public void setFhpReleaseCount(int fhpReleaseCount) {
			this.fhpReleaseCount = fhpReleaseCount;
		}
		
		public void setFhpWaitCount(int fhpWaitCount) {
			this.fhpWaitCount = fhpWaitCount;
		}
		
		public void setPageGetValWaitCount(int pageGetValWaitCount) {
			this.pageGetValWaitCount = pageGetValWaitCount;
		}
		
		public void setPageSetValWaitCount(int pageSetValWaitCount) {
			this.pageSetValWaitCount = pageSetValWaitCount;
		}
		
		public void setPageGetValReleaseCount(int pageGetValReleaseCount) {
			this.pageGetValReleaseCount = pageGetValReleaseCount;
		}
		
		public void setPageSetValReleaseCount(int pageSetValReleaseCount) {
			this.pageSetValReleaseCount = pageSetValReleaseCount;
		}
		
		public void setSystemCpuLoadTicks(long[] systemCpuLoadTicks) {
			this.systemCpuLoadTicks = systemCpuLoadTicks;
		}
		
		public void setSystemLoadAverage(double systemLoadAverage) {
			this.systemLoadAverage = systemLoadAverage;
		}
		
		public void setProcessUserTime(long processUserTime) {
			this.processUserTime = processUserTime;
		}
		
		public void setProcessKernelTime(long processKernelTime) {
			this.processKernelTime = processKernelTime;
		}
		
		public void setProcessUpTime(long processUpTime) {
			this.processUpTime = processUpTime;
		}
		
		public void setThreadActiveCount(int threadActiveCount) {
			this.threadActiveCount = threadActiveCount;
		}
		
		public void setIOReadBytes(long ioReadBytes) {
			this.ioReadBytes = ioReadBytes;
		}
		
		public void setIOWriteBytes(long ioWriteBytes) {
			this.ioWriteBytes = ioWriteBytes;
		}
		
		public void setIOQueueLength(long ioQueueLength) {
			this.ioQueueLength = ioQueueLength;
		}
		
		public TPartSystemMetrics build() {
			TPartSystemMetrics metrics = new TPartSystemMetrics();
			metrics.serverId = serverId;
			
			metrics.bufferHitRate = bufferHitRate;
			metrics.bufferAvgPinCount = bufferAvgPinCount;
			metrics.pinnedBufferCount = pinnedBufferCount;
			
			metrics.bufferReadWaitCount = bufferReadWaitCount;
			metrics.bufferWriteWaitCount = bufferWriteWaitCount;
			metrics.blockReleaseCount = blockReleaseCount;
			metrics.blockWaitCount = blockWaitCount;
			metrics.fhpReleaseCount = fhpReleaseCount;
			metrics.fhpWaitCount = fhpWaitCount;
			metrics.pageGetValWaitCount = pageGetValWaitCount;
			metrics.pageSetValWaitCount = pageSetValWaitCount;
			metrics.pageGetValReleaseCount = pageGetValReleaseCount;
			metrics.pageSetValReleaseCount = pageSetValReleaseCount;
			
			metrics.systemCpuLoadTicks = systemCpuLoadTicks;
			metrics.systemLoadAverage = systemLoadAverage;
			metrics.processUserTime = processUserTime;
			metrics.processKernelTime = processKernelTime;
			metrics.processUpTime = processUpTime;
			
			metrics.threadActiveCount = threadActiveCount;
			
			metrics.ioReadBytes = ioReadBytes;
			metrics.ioWriteBytes = ioWriteBytes;
			metrics.ioQueueLength = ioQueueLength;
			
			return metrics;
		}
	}
	
	private int serverId;

	private double bufferHitRate;
	private double bufferAvgPinCount;
	private int pinnedBufferCount;
	
	private int bufferReadWaitCount;
	private int bufferWriteWaitCount;
	private int blockReleaseCount;
	private int blockWaitCount;
	private int fhpReleaseCount;
	private int fhpWaitCount;
	private int pageGetValWaitCount;
	private int pageSetValWaitCount;
	private int pageGetValReleaseCount;
	private int pageSetValReleaseCount;
	
	// CPU Usage and Loading
	private long[] systemCpuLoadTicks;
	private double systemLoadAverage;
	private long processUserTime;
	private long processKernelTime;
	private long processUpTime;
	
	private int threadActiveCount;
	
	private long ioReadBytes;
	private long ioWriteBytes;
	private long ioQueueLength;
	
	private TPartSystemMetrics() {
		// do nothing
	}
	
	public int getServerId() {
		return serverId;
	}
	
	public double getBufferHitRate() {
		return bufferHitRate;
	}
	
	public double getBufferAvgPinCount() {
		return bufferAvgPinCount;
	}
	
	public int getPinnedBufferCount() {
		return pinnedBufferCount;
	}
	
	public int getBufferReadWaitCount() {
		return bufferReadWaitCount;
	}
	
	public int getBufferWriteWaitCount() {
		return bufferWriteWaitCount;
	}
	
	public int getBlockReleaseCount() {
		return blockReleaseCount;
	}
	
	public int getBlockWaitCount() {
		return blockWaitCount;
	}
	
	public int getFhpReleaseCount() {
		return fhpReleaseCount;
	}
	
	public int getFhpWaitCount() {
		return fhpWaitCount;
	}
	
	public int getPageGetValWaitCount() {
		return pageGetValWaitCount;
	}
	
	public int getPageSetValWaitCount() {
		return pageSetValWaitCount;
	}
	
	public int getPageGetValReleaseCount() {
		return pageGetValReleaseCount;
	}
	
	public int getPageSetValReleaseCount() {
		return pageSetValReleaseCount;
	}
	
	public long[] getSystemCpuLoadTicks() {
		return systemCpuLoadTicks;
	}
	
	public double getSystemLoadAverage() {
		return systemLoadAverage;
	}
	
	public long getProcessUserTime() {
		return processUserTime;
	}
	
	public long getProcessKernelTime() {
		return processKernelTime;
	}
	
	public long getProcessUpTime() {
		return processUpTime;
	}
	
	public int getThreadActiveCount() {
		return threadActiveCount;
	}
	
	public long getIOReadBytes() {
		return ioReadBytes;
	}
	
	public long getIOWriteBytes() {
		return ioWriteBytes;
	}
	
	public long getIOQueueLength() {
		return ioQueueLength;
	}
}
