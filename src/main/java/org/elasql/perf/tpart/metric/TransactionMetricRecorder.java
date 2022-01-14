package org.elasql.perf.tpart.metric;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
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
 * @author Yu-Shan Lin, Yu-Xuan Lin, Ping-Yu Wang
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
	private BlockingQueue<TransactionMetrics> queue = new LinkedBlockingQueue<TransactionMetrics>();

	// Header
	private int serverId;
	private int columnCount;
	private List<Object> metricNames = new ArrayList<Object>();
	private Map<Object, Integer> metricNameToPos = new HashMap<Object, Integer>();

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

	@SuppressWarnings("unused")
	private class CsvSavers implements AutoCloseable {
		public CsvSaver<LongValueRow> latencyCsvSaver;
		public CsvSaver<LongValueRow> cpuTimeCsvSaver;
		public CsvSaver<LongValueRow> diskioCountCsvSaver;
		public CsvSaver<LongValueRow> networkinSizeCsvSaver;
		public CsvSaver<LongValueRow> networkoutSizeCsvSaver;

		public BufferedWriter latencyWriter;
		public BufferedWriter cpuTimeWriter;
		public BufferedWriter diskioCountWriter;
		public BufferedWriter networkinSizeWriter;
		public BufferedWriter networkoutSizeWriter;

		public CsvSavers() throws IOException {
			initLatencyCsvSaver();
			if (ENABLE_CPU_TIMER) {
				initCpuTimeCsvSaver();
			}
			if (ENABLE_DISKIO_COUNTER) {
				initDiskioCountCsvSaver();
			}
			if (ENABLE_NETWORKIO_COUNTER) {
				initNetworkinSizeCsvSaver();
				initNetworkoutSizeCsvSaver();
			}
		}

		private void initLatencyCsvSaver() throws IOException {
			String fileName = String.format("%s-latency-server-%d", FILENAME_PREFIX, serverId);
			latencyCsvSaver = new CsvSaver<LongValueRow>(fileName);
			latencyWriter = latencyCsvSaver.createOutputFile();
		}

		private void initCpuTimeCsvSaver() throws IOException {
			String fileName = String.format("%s-cpu-time-server-%d", FILENAME_PREFIX, serverId);
			cpuTimeCsvSaver = new CsvSaver<LongValueRow>(fileName);
			cpuTimeWriter = cpuTimeCsvSaver.createOutputFile();
		}

		private void initDiskioCountCsvSaver() throws IOException {
			String fileName = String.format("%s-diskio-count-server-%d", FILENAME_PREFIX, serverId);
			diskioCountCsvSaver = new CsvSaver<LongValueRow>(fileName);
			diskioCountWriter = diskioCountCsvSaver.createOutputFile();
		}

		private void initNetworkinSizeCsvSaver() throws IOException {
			String fileName = String.format("%s-networkin-size-server-%d", FILENAME_PREFIX, serverId);
			networkinSizeCsvSaver = new CsvSaver<LongValueRow>(fileName);
			networkinSizeWriter = networkinSizeCsvSaver.createOutputFile();
		}

		private void initNetworkoutSizeCsvSaver() throws IOException {
			String fileName = String.format("%s-networkout-size-server-%d", FILENAME_PREFIX, serverId);
			networkoutSizeCsvSaver = new CsvSaver<LongValueRow>(fileName);
			networkoutSizeWriter = networkoutSizeCsvSaver.createOutputFile();
		}

		@Override
		public void close() throws IOException {
			latencyWriter.close();
			if (ENABLE_CPU_TIMER) {
				cpuTimeWriter.close();
			}
			if (ENABLE_DISKIO_COUNTER) {
				diskioCountWriter.close();
			}
			if (ENABLE_NETWORKIO_COUNTER) {
				networkinSizeWriter.close();
				networkoutSizeWriter.close();
			}
		}

		public void printInfo() {
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("A latency log is generated at '%s'", latencyCsvSaver.fileName());
				logger.info(log);
				if (ENABLE_CPU_TIMER) {
					log = String.format("A cpu time log is generated at '%s'", cpuTimeCsvSaver.fileName());
					logger.info(log);
				}
				if (ENABLE_DISKIO_COUNTER) {
					log = String.format("A disk io count log is generated at '%s'", diskioCountCsvSaver.fileName());
					logger.info(log);
				}
				if (ENABLE_NETWORKIO_COUNTER) {
					log = String.format("A network in size log is generated at '%s'", networkinSizeCsvSaver.fileName());
					logger.info(log);
					log = String.format("A network out size log is generated at '%s'",
							networkoutSizeCsvSaver.fileName());
					logger.info(log);
				}
			}
		}
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Metrics Recorder");
		try {
			// Wait for receiving the first metrics
			TransactionMetrics metrics = queue.take();

			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction metrics recorder starts recording metrics");

			try (CsvSavers savers = new CsvSavers()) {
				// Save the first metrics
				saveMetrics(savers, metrics);

				// Wait until no more metrics coming in the last 10 seconds
				while ((metrics = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
					saveMetrics(savers, metrics);
				}

				if (logger.isLoggable(Level.INFO)) {
					String log = String.format("No more metrics coming in last %d seconds. Start generating a report.",
							TIME_TO_FLUSH);
					logger.info(log);
				}

				savers.printInfo();

			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void saveHeader(CsvSavers savers, TransactionMetrics metrics) throws IOException {
		// Generate the header
		List<String> header = generateHeader(metrics);

		savers.latencyCsvSaver.writeHeader(savers.latencyWriter, header);
		if (ENABLE_CPU_TIMER) {
			savers.cpuTimeCsvSaver.writeHeader(savers.cpuTimeWriter, header);
		}
		if (ENABLE_DISKIO_COUNTER) {
			savers.diskioCountCsvSaver.writeHeader(savers.diskioCountWriter, header);
		}
		if (ENABLE_NETWORKIO_COUNTER) {
			savers.networkinSizeCsvSaver.writeHeader(savers.networkinSizeWriter, header);
			savers.networkoutSizeCsvSaver.writeHeader(savers.networkoutSizeWriter, header);
		}
	}

	private void saveMetrics(CsvSavers savers, TransactionMetrics metrics) throws IOException {
		
		if (updateMetricNames(metrics)) {
			saveHeader(savers, metrics);
		}
		
		// Split the metrics to different types of rows
		LongValueRow latRow = convertToLatencyRow(metrics);
		savers.latencyCsvSaver.writeRecord(savers.latencyWriter, latRow, columnCount);
		if (ENABLE_CPU_TIMER) {
			LongValueRow cpuRow = convertToCpuTimeRow(metrics);
			savers.cpuTimeCsvSaver.writeRecord(savers.cpuTimeWriter, cpuRow, columnCount);
		}
		if (ENABLE_DISKIO_COUNTER) {
			LongValueRow diskioRow = convertToDiskioCountRow(metrics);
			savers.diskioCountCsvSaver.writeRecord(savers.diskioCountWriter, diskioRow, columnCount);
		}
		if (ENABLE_NETWORKIO_COUNTER) {
			LongValueRow networkinRow = convertToNetworkinSizeRow(metrics);
			savers.networkinSizeCsvSaver.writeRecord(savers.networkinSizeWriter, networkinRow, columnCount);
			LongValueRow networkoutRow = convertToNetworkoutSizeRow(metrics);
			savers.networkoutSizeCsvSaver.writeRecord(savers.networkoutSizeWriter, networkoutRow, columnCount);
		}
	}
	
	private boolean updateMetricNames(TransactionMetrics metrics) {
		boolean headerChanged = false;
		for (String metricName : metrics.metricNames) {
			if (!metricNameToPos.containsKey(metricName)) {
				headerChanged = true;
				metricNames.add(metricName);
				metricNameToPos.put(metricName, metricNames.size() - 1);
			}
		}
		return headerChanged;
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
				diskioValues[pos] = metrics.diskioCounts.get(metricName);
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
				networkinValues[pos] = metrics.networkinSizes.get(metricName);
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
				networkoutValues[pos] = metrics.networkoutSizes.get(metricName);
			}
		}
		return new LongValueRow(metrics.txNum, metrics.isMaster, metrics.isTxDistributed, networkoutValues);
	}

	private List<String> generateHeader(TransactionMetrics metrics) {

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

		columnCount = header.size();

		return header;
	}
}
