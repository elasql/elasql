package org.elasql.perf.tpart.metric;

import java.lang.management.ManagementFactory;
import java.util.List;

import org.elasql.perf.tpart.TPartPerformanceManager;
import org.elasql.server.Elasql;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.buffer.BufferPoolMonitor;
import org.vanilladb.core.util.TransactionProfiler;

import com.sun.management.OperatingSystemMXBean;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.HWDiskStore;

/**
 * A collector that collects system and transaction metrics on each machine.
 * 
 * @author Yu-Shan Lin
 */
@SuppressWarnings("restriction")
public class MetricCollector extends Task {

	private static final int SYSTEM_METRIC_INTERVAL = 100; // in milliseconds

	private TransactionMetricRecorder metricRecorder;
	
	private CentralProcessor cpu;
	private long[] cpuTicks;
	
	private HWDiskStore hwds;
	private long previousReadByte = 0l;
	private long previousWriteByte = 0l;
	
	private OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
		.getOperatingSystemMXBean();

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
		HardwareAbstractionLayer hal = si.getHardware();
		cpu = hal.getProcessor();
		cpuTicks = cpu.getSystemCpuLoadTicks();
	}

	private void initDiskStore() {
		hwds = new SystemInfo().getHardware().getDiskStores().get(0);
	}

	private TPartSystemMetrics collectSystemMetrics() {
		TPartSystemMetrics.Builder builder = new TPartSystemMetrics.Builder(Elasql.serverId());

		builder.setBufferHitRate(BufferPoolMonitor.getHitRate());
		builder.setBufferAvgPinCount(BufferPoolMonitor.getAvgPinCount());
		builder.setPinnedBufferCount(BufferPoolMonitor.getPinnedBufferCount());
		
		builder.setProcessCpuLoad(bean.getProcessCpuLoad());
		builder.setSystemCpuLoad(cpu.getSystemCpuLoadBetweenTicks(cpuTicks));
		cpuTicks = cpu.getSystemCpuLoadTicks();
		builder.setSystemLoadAverage(bean.getSystemLoadAverage());
		builder.setThreadActiveCount(getThreadActiveCount());
		
		builder.setIOReadByte(getIOReadByte());
		builder.setIOWriteByte(getIOWriteByte());
		builder.setIOQueueLength(getIOQueuLangth());
		return builder.build();
	}
	
	private int getThreadActiveCount() {
		return Elasql.taskMgr().getActiveCount();
	}
	
	private long getIOReadByte() {
		long readByte = hwds.getReadBytes() - previousReadByte;
		previousReadByte = hwds.getReadBytes();
		return readByte;
	}
	private long getIOWriteByte() {
		long writeByte = hwds.getWriteBytes() - previousWriteByte;
		previousWriteByte = hwds.getWriteBytes();
		return writeByte;
	}
	private long getIOQueuLangth() {
		return hwds.getCurrentQueueLength();
	}
}
