package org.elasql.perf.tpart.metric;

import org.elasql.perf.tpart.TPartPerformanceManager;
import org.elasql.server.Elasql;
import org.elasql.storage.tx.concurrency.ConservativeOrderedLockMonitor;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferPoolMonitor;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.util.TransactionProfiler;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

/**
 * A collector that collects system and transaction metrics on each machine.
 * 
 * @author Yu-Shan Lin
 */
public class MetricCollector extends Task {
	private static final int SYSTEM_METRIC_INTERVAL = 1000; // in milliseconds

	private TransactionMetricRecorder metricRecorder;
	
	private OperatingSystem os;
	private CentralProcessor cpu;
	
	private HWDiskStore hwds;
	private long previousReadBytes = 0l;
	private long previousWriteBytes = 0l;

	public MetricCollector() {
		if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
			metricRecorder = new TransactionMetricRecorder(Elasql.serverId());
			metricRecorder.startRecording();
		}
	}

	public void addTransactionMetrics(long txNum, String role, boolean isTxDistributed, TransactionProfiler profiler) {
		if (!TPartPerformanceManager.ENABLE_COLLECTING_DATA)
			throw new IllegalStateException("cannot collect transaction metrics since ENABLE_COLLECTING_DATA = false");
	
		metricRecorder.addTransactionMetrics(txNum, role, isTxDistributed, profiler);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("metric-collector");
		long startTime;
		
		setupInterfaces();

		try {
			while (true) {
				startTime = System.nanoTime();

				// Read disk info 
				initDiskStore();
				
				// Collect the metrics
				TPartSystemMetrics metrics = collectSystemMetrics();
				
				// Send to the sequencer
				if (Elasql.connectionMgr() != null)
					Elasql.connectionMgr().sendMetricReport(metrics);

				// Wait for the next collection
				while ((System.nanoTime() - startTime) / 1000_000 < SYSTEM_METRIC_INTERVAL) {
					Thread.sleep(SYSTEM_METRIC_INTERVAL / 10);
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void setupInterfaces() {
		SystemInfo si = new SystemInfo();
		os = si.getOperatingSystem();
		HardwareAbstractionLayer hal = si.getHardware();
		cpu = hal.getProcessor();
	}

	private void initDiskStore() {
		hwds = new SystemInfo().getHardware().getDiskStores().get(0);
	}

	private TPartSystemMetrics collectSystemMetrics() {
		TPartSystemMetrics.Builder builder = new TPartSystemMetrics.Builder(Elasql.serverId());

		builder.setBufferHitRate(BufferPoolMonitor.getHitRate());
		builder.setBufferAvgPinCount(BufferPoolMonitor.getAvgPinCount());
		builder.setPinnedBufferCount(BufferPoolMonitor.getPinnedBufferCount());
		
		builder.setBufferReadWaitCount(BufferPoolMonitor.getReadWaitCount());
		builder.setBufferWriteWaitCount(BufferPoolMonitor.getWriteWaitCount());
		builder.setBlockReleaseCount(BufferPoolMonitor.getBlockReleaseCount());
		builder.setBlockWaitCount(BufferPoolMonitor.getBlockWaitCount());
		builder.setFhpReleaseCount(RecordFile.fhpReleaseCount());
		builder.setFhpWaitCount(RecordFile.fhpWaitCount());
		builder.setPageGetValReleaseCount(Buffer.getPageGetValWaitCount());
		builder.setPageSetValReleaseCount(Buffer.getPageSetValWaitCount());
		builder.setPageGetValReleaseCount(Buffer.getPageGetValReleaseCount());
		builder.setPageSetValReleaseCount(Buffer.getPageSetValReleaseCount());
		
		collectCpuLoad(builder);
		builder.setThreadActiveCount(getThreadActiveCount());
		
		builder.setIOReadBytes(getIOReadBytes());
		builder.setIOWriteBytes(getIOWriteBytes());
		builder.setIOQueueLength(getIOQueuLangth());
		
		collectLatch(builder);
		
		return builder.build();
	}
	
	private void collectCpuLoad(TPartSystemMetrics.Builder builder) {
		// System
		builder.setSystemCpuLoadTicks(cpu.getSystemCpuLoadTicks());
		builder.setSystemLoadAverage(cpu.getSystemLoadAverage(1)[0]);
		
		// Process
		OSProcess process = os.getProcess(os.getProcessId());
		builder.setProcessUserTime(process.getUserTime());
		builder.setProcessKernelTime(process.getKernelTime());
		builder.setProcessUpTime(process.getUpTime());
	}
	
	private void collectLatch(TPartSystemMetrics.Builder builder) {
		// xLock
		builder.setxLockSimpleMovingAverage(ConservativeOrderedLockMonitor.getxLockWaitTimeSMA());
	}
	
	private int getThreadActiveCount() {
		return Elasql.taskMgr().getActiveCount();
	}
	
	private long getIOReadBytes() {
		long readBytes = hwds.getReadBytes() - previousReadBytes;
		previousReadBytes = hwds.getReadBytes();
		return readBytes;
	}
	private long getIOWriteBytes() {
		long writeBytes = hwds.getWriteBytes() - previousWriteBytes;
		previousWriteBytes = hwds.getWriteBytes();
		return writeBytes;
	}
	private long getIOQueuLangth() {
		return hwds.getCurrentQueueLength();
	}
}
