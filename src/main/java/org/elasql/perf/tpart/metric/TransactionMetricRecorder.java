package org.elasql.perf.tpart.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;
import org.vanilladb.core.server.VanillaDb;
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
	private static final long TIME_TO_FLUSH = 10; // in seconds
	
	private static class TransactionMetrics {
		long txNum;
		boolean isMaster;
		List<String> metricNames;
		Map<String, Long> latencies;
		
		TransactionMetrics(long txNum, String role, TransactionProfiler profiler) {
			this.txNum = txNum;
			this.isMaster = role.equals("Master");
			
			metricNames = new ArrayList<String>();
			latencies = new HashMap<String, Long>();
			for (Object component : profiler.getComponents()) {
				String metricName = component.toString();
				metricNames.add(metricName);
				latencies.put(metricName, profiler.getComponentTime(component));
			}
		}
	}
	
	private static class LatencyRow implements CsvRow, Comparable<LatencyRow> {
		long txNum;
		boolean isMaster;
		long[] values;
		
		LatencyRow(long txNum, boolean isMaster, long[] values) {
			this.txNum = txNum;
			this.isMaster = isMaster;
			this.values = values;
		}

		@Override
		public String getVal(int index) {
			if (index == 0) {
				return Long.toString(txNum);
			} else if (index == 1) {
				return Boolean.toString(isMaster);
			} else {
				if (index - 2 < values.length) {
					return Long.toString(values[index - 2]);
				} else {
					return "";
				}
			}
		}

		@Override
		public int compareTo(LatencyRow row) {
			return Long.compare(txNum, row.txNum);
		}
	}
	
	private AtomicBoolean isRecording = new AtomicBoolean(false);
	private BlockingQueue<TransactionMetrics> queue
		= new ArrayBlockingQueue<TransactionMetrics>(100000);
	
	// Header
	private int serverId;
	private List<Object> metricNames = new ArrayList<Object>();
	private Map<Object, Integer> metricNameToPos = new HashMap<Object, Integer>();
	
	// Data
	private List<LatencyRow> latencyRows = new ArrayList<LatencyRow>();
	
	public TransactionMetricRecorder(int serverId) {
		this.serverId = serverId;
	}
	
	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			VanillaDb.taskMgr().runTask(this);
		}
	}
	
	public void addTransactionMetrics(long txNum, String role, TransactionProfiler profiler) {
		if (!isRecording.get())
			return;
		
		queue.add(new TransactionMetrics(txNum, role, profiler));
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
		LatencyRow latRow = convertToLatencyRow(metrics);
		latencyRows.add(latRow);
	}
	
	private void updateMetricNames(TransactionMetrics metrics) {
		for (String metricName : metrics.metricNames) {
			if (!metricNameToPos.containsKey(metricName)) {
				metricNames.add(metricName);
				metricNameToPos.put(metricName, metricNames.size() - 1);
			}
		}
	}
	
	private LatencyRow convertToLatencyRow(TransactionMetrics metrics) {
		long[] values = new long[metricNames.size()];
		for (Object metricName : metricNames) {
			Long latency = metrics.latencies.get(metricName);
			if (latency != null) {
				int pos = metricNameToPos.get(metricName);
				values[pos] = latency;
			}
		}
		return new LatencyRow(metrics.txNum, metrics.isMaster, values);
	}
	
	private void sortRows() {
		Collections.sort(latencyRows);
	}
	
	private void saveToCsv() {
		// Generate the header
		List<String> header = generateHeader();
		
		// Save to different files
		saveToLatencyCsv(header);
	}
	
	private void saveToLatencyCsv(List<String> header) {
		String fileName = String.format("%s-latency-server-%d", FILENAME_PREFIX, serverId);
		CsvSaver<LatencyRow> csvSaver = new CsvSaver<LatencyRow>(fileName);
		String path = csvSaver.generateOutputFile(header, latencyRows);
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A latency log is generated at '%s'", path);
			logger.info(log);
		}
	}
	
	
	private List<String> generateHeader() {
		List<String> header = new ArrayList<String>();
		
		// First column: transaction ID
		header.add(TRANSACTION_ID_COLUMN);
		
		// Second column: is master
		header.add(MASTER_ID_COLUMN);
		
		// After: metrics
		for (Object metricName : metricNames)
			header.add(metricName.toString());
		
		return header;
	}
}
