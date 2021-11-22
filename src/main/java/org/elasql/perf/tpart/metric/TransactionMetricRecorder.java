package org.elasql.perf.tpart.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * A recorder to record the transaction metrics to CSV files.
 * 
 * @author Yu-Shan Lin
 */
public class TransactionMetricRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionMetricRecorder.class.getName());
	
	private static final String FILENAME_PREFIX = "transaction";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	private static final String MASTER_ID_COLUMN = "Is Master";
	private static final String DISTRIBUTED_TX_COLUMN = "Is Distributed";
	private static final long TIME_TO_FLUSH = 10; // in seconds
	private static final boolean ENABLE_CPU_TIMER = TransactionProfiler.ENABLE_CPU_TIMER;
	private static final boolean ENABLE_DISKIO_COUNTER = TransactionProfiler.ENABLE_DISKIO_COUNTER;
	private static final boolean ENABLE_NETWORKIO_COUNTER = TransactionProfiler.ENABLE_NETWORKIO_COUNTER;
	
	private static class TransactionMetrics {
		long txNum;
		boolean isMaster;
		boolean isTxDistributed;
		List<String> metricNames;
		Map<String, Long> latencies;
		Map<String, Long> cpuTimes;
		Map<String, Long> diskioCounts;
		Map<String, Long> networkinSizes;
		Map<String, Long> networkoutSizes;
		
		TransactionMetrics(long txNum, String role, boolean isTxDistributed, TransactionProfiler profiler) {
			this.txNum = txNum;
			this.isMaster = role.equals("Master");
			this.isTxDistributed = isTxDistributed;
			
			metricNames = new ArrayList<String>();
			latencies = new HashMap<String, Long>();
			cpuTimes = new HashMap<String, Long>();
			diskioCounts = new HashMap<String, Long>();
			networkinSizes = new HashMap<String, Long>();
			networkoutSizes = new HashMap<String, Long>();
			for (Object component : profiler.getComponents()) {
				String metricName = component.toString();
				metricNames.add(metricName);
				latencies.put(metricName, profiler.getComponentTime(component));
				if (ENABLE_CPU_TIMER)
					cpuTimes.put(metricName, profiler.getComponentCpuTime(component));
				if (ENABLE_DISKIO_COUNTER)
					diskioCounts.put(metricName, profiler.getComponentDiskIOCount(component));
				if (ENABLE_NETWORKIO_COUNTER) {
					networkinSizes.put(metricName, profiler.getComponentNetworkInSize(component));
					networkoutSizes.put(metricName, profiler.getComponentNetworkOutSize(component));
				}
					
			}
		}
	}
	
	private static class LongValueRow implements CsvRow, Comparable<LongValueRow> {
		long txNum;
		boolean isMaster;
		boolean isTxDistributed;
		long[] values;
		
		LongValueRow(long txNum, boolean isMaster, boolean isTxDistributed, long[] values) {
			this.txNum = txNum;
			this.isMaster = isMaster;
			this.isTxDistributed = isTxDistributed;
			this.values = values;
		}

		@Override
		public String getVal(int index) {
			if (index == 0) {
				return Long.toString(txNum);
			} else if (index == 1) {
				return Boolean.toString(isMaster);
			} else if (index == 2) {
				return Boolean.toString(isTxDistributed);
			} else {
				if (index - 3 < values.length) {
					return Long.toString(values[index - 3]);
				} else {
					return "0";
				}
			}
		}

		@Override
		public int compareTo(LongValueRow row) {
			return Long.compare(txNum, row.txNum);
		}
	}
	
	private AtomicBoolean isRecording = new AtomicBoolean(false);
	private BlockingQueue<TransactionMetrics> queue
		= new LinkedBlockingQueue<TransactionMetrics>();
	
	// Header
	private int serverId;
	private List<Object> metricNames = new ArrayList<Object>();
	private Map<Object, Integer> metricNameToPos = new HashMap<Object, Integer>();
	
	// Data
	private List<LongValueRow> latencyRows = new ArrayList<LongValueRow>();
	private List<LongValueRow> cpuTimeRows = new ArrayList<LongValueRow>();
	private List<LongValueRow> diskioCountRows = new ArrayList<LongValueRow>();
	private List<LongValueRow> networkinSizeRows = new ArrayList<LongValueRow>();
	private List<LongValueRow> networkoutSizeRows = new ArrayList<LongValueRow>();
	
	public TransactionMetricRecorder(int serverId) {
		this.serverId = serverId;
	}
	
	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			Elasql.taskMgr().runTask(this);
		}
	}
	
	public void addTransactionMetrics(long txNum, String role, boolean isTxDistributed, TransactionProfiler profiler) {
		if (!isRecording.get())
			return;
		
		queue.add(new TransactionMetrics(txNum, role, isTxDistributed, profiler));
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Metrics Recorder");
		
		try {
			// Wait for receiving the first metrics
			TransactionMetrics metrics = queue.take();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction metrics recorder starts recording metrics");
			
			// Save the metrics
			saveMetrics(metrics);
			
			// Wait until no more metrics coming in the last 10 seconds
			while ((metrics = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				saveMetrics(metrics);
			}
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more metrics coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}
			
			// Sort by transaction ID
			sortRows();
			
			// Save to CSV files
			saveToCsv();
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void saveMetrics(TransactionMetrics metrics) {
		// Update component names
		updateMetricNames(metrics);
		
		// Split the metrics to different types of rows
		LongValueRow latRow = convertToLatencyRow(metrics);
		latencyRows.add(latRow);
		
		if (ENABLE_CPU_TIMER) {
			LongValueRow cpuRow = convertToCpuTimeRow(metrics);
			cpuTimeRows.add(cpuRow);
		}
		if (ENABLE_DISKIO_COUNTER) {
			LongValueRow diskioRow = convertToDiskioCountRow(metrics);
			diskioCountRows.add(diskioRow);
		}
		if (ENABLE_NETWORKIO_COUNTER) {
			LongValueRow networkinRow = convertToNetworkinSizeRow(metrics);
			networkinSizeRows.add(networkinRow);
			LongValueRow networkoutRow = convertToNetworkoutSizeRow(metrics);
			networkoutSizeRows.add(networkoutRow);
		}
		
	}
	
	private void updateMetricNames(TransactionMetrics metrics) {
		for (String metricName : metrics.metricNames) {
			if (!metricNameToPos.containsKey(metricName)) {
				metricNames.add(metricName);
				metricNameToPos.put(metricName, metricNames.size() - 1);
			}
		}
	}
	
	private LongValueRow convertToLatencyRow(TransactionMetrics metrics) {
		long[] latValues = new long[metricNames.size()];
		
		for (Object metricName : metricNames) {
			Long latency = metrics.latencies.get(metricName);
			if (latency != null) {
				int pos = metricNameToPos.get(metricName);
				latValues[pos] = latency;
			}
		}
		return new LongValueRow(metrics.txNum, metrics.isMaster, metrics.isTxDistributed, latValues);
	}
	
	private LongValueRow convertToCpuTimeRow(TransactionMetrics metrics) {
		long[] cpuValues = new long[metricNames.size()];
		
		for (Object metricName : metricNames) {
			Long cpuTime = metrics.cpuTimes.get(metricName);
			if (cpuTime != null) {
				int pos = metricNameToPos.get(metricName);
				cpuValues[pos] = metrics.cpuTimes.get(metricName);
			}
		}
		return new LongValueRow(metrics.txNum, metrics.isMaster, metrics.isTxDistributed, cpuValues);
	}
	
	private LongValueRow convertToDiskioCountRow(TransactionMetrics metrics) {
		long[] diskioValues = new long[metricNames.size()];
		
		for (Object metricName : metricNames) {
			Long diskioCount = metrics.diskioCounts.get(metricName);
			if (diskioCount != null) {
				int pos = metricNameToPos.get(metricName);
				diskioValues[pos] = metrics.diskioCounts.get(metricName);;
			}
		}
		return new LongValueRow(metrics.txNum, metrics.isMaster, metrics.isTxDistributed, diskioValues);
	}
	
	private LongValueRow convertToNetworkinSizeRow(TransactionMetrics metrics) {
		long[] networkinValues = new long[metricNames.size()];
		
		for (Object metricName : metricNames) {
			Long networkinSize = metrics.networkinSizes.get(metricName);
			if (networkinSize != null) {
				int pos = metricNameToPos.get(metricName);
				networkinValues[pos] = metrics.networkinSizes.get(metricName);;
			}
		}
		return new LongValueRow(metrics.txNum, metrics.isMaster, metrics.isTxDistributed, networkinValues);
	}

	private LongValueRow convertToNetworkoutSizeRow(TransactionMetrics metrics) {
		long[] networkoutValues = new long[metricNames.size()];
		
		for (Object metricName : metricNames) {
			Long networkoutSize = metrics.networkoutSizes.get(metricName);
			if (networkoutSize != null) {
				int pos = metricNameToPos.get(metricName);
				networkoutValues[pos] = metrics.networkoutSizes.get(metricName);;
			}
		}
		return new LongValueRow(metrics.txNum, metrics.isMaster, metrics.isTxDistributed, networkoutValues);
	}
	
	private void sortRows() {
		Collections.sort(latencyRows);
		if (ENABLE_CPU_TIMER)
			Collections.sort(cpuTimeRows);
		if (ENABLE_DISKIO_COUNTER)
			Collections.sort(diskioCountRows);
		if (ENABLE_NETWORKIO_COUNTER) {
			Collections.sort(networkinSizeRows);
			Collections.sort(networkoutSizeRows);
		}
	}
	
	private void saveToCsv() {
		// Generate the header
		List<String> header = generateHeader();
		
		// Save to different files
		saveToLatencyCsv(header);
		if (ENABLE_CPU_TIMER)
			saveToCpuTimeCsv(header);
		if (ENABLE_DISKIO_COUNTER)
			saveToDiskioCountCsv(header);
		if (ENABLE_NETWORKIO_COUNTER) {
			saveToNetworkinSizeCsv(header);
			saveToNetworkoutSizeCsv(header);
		}		
	}
	
	private void saveToLatencyCsv(List<String> header) {
		String fileName = String.format("%s-latency-server-%d", FILENAME_PREFIX, serverId);
		CsvSaver<LongValueRow> csvSaver = new CsvSaver<LongValueRow>(fileName);
		String path = csvSaver.generateOutputFile(header, latencyRows);
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A latency log is generated at '%s'", path);
			logger.info(log);
		}
	}
	
	private void saveToCpuTimeCsv(List<String> header) {
		String fileName = String.format("%s-cpu-time-server-%d", FILENAME_PREFIX, serverId);
		CsvSaver<LongValueRow> csvSaver = new CsvSaver<LongValueRow>(fileName);
		String path = csvSaver.generateOutputFile(header, cpuTimeRows);
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A cpu time log is generated at '%s'", path);
			logger.info(log);
		}
	}	
	
	private void saveToDiskioCountCsv(List<String> header) {
		String fileName = String.format("%s-diskio-count-server-%d", FILENAME_PREFIX, serverId);
		CsvSaver<LongValueRow> csvSaver = new CsvSaver<LongValueRow>(fileName);
		String path = csvSaver.generateOutputFile(header, diskioCountRows);
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A disk io count log is generated at '%s'", path);
			logger.info(log);
		}
	}
	
	private void saveToNetworkinSizeCsv(List<String> header) {
		String fileName = String.format("%s-networkin-size-server-%d", FILENAME_PREFIX, serverId);
		CsvSaver<LongValueRow> csvSaver = new CsvSaver<LongValueRow>(fileName);
		String path = csvSaver.generateOutputFile(header, networkinSizeRows);
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A network in size log is generated at '%s'", path);
			logger.info(log);
		}
	}
	
	private void saveToNetworkoutSizeCsv(List<String> header) {
		String fileName = String.format("%s-networkout-size-server-%d", FILENAME_PREFIX, serverId);
		CsvSaver<LongValueRow> csvSaver = new CsvSaver<LongValueRow>(fileName);
		String path = csvSaver.generateOutputFile(header, networkoutSizeRows);
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A network out size log is generated at '%s'", path);
			logger.info(log);
		}
	}
	
	private List<String> generateHeader() {
		List<String> header = new ArrayList<String>();
		
		// First column: transaction ID
		header.add(TRANSACTION_ID_COLUMN);
		
		// Second column: is master
		header.add(MASTER_ID_COLUMN);
		
		// Third column: is tx distributed
		header.add(DISTRIBUTED_TX_COLUMN);
		
		// After: metrics
		for (Object metricName : metricNames)
			header.add(metricName.toString());
		
		return header;
	}
}
