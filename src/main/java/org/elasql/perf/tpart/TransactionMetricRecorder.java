package org.elasql.perf.tpart;

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

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

/**
 * A recorder to record the transaction metrics to a CSV file.
 * 
 * @author Yu-Shan Lin
 */
public class TransactionMetricRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionMetricRecorder.class.getName());
	
	private static final String FILENAME_PREFIX = "transaction-metrics";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	private static final String MASTER_ID_COLUMN = "Master Node ID";
	private static final long TIME_TO_FLUSH = 10; // in seconds
	
	private static class MetricRow implements CsvRow, Comparable<MetricRow> {
		
		private TransactionMetricRecorder recorder;
		private long txNum;
		private int masterId = -1;
		// [Each Server][Each Metric]
		private Object[][] metricValues;
		
		MetricRow(TransactionMetricRecorder recorder, long txNum) {
			this.recorder = recorder;
			this.txNum = txNum;
			this.metricValues = new Object[PartitionMetaMgr.NUM_PARTITIONS][];
		}
		
		void addMetricValueForServer(int serverId, boolean isMaster, Object[] metricValues) {
			// Check if the metrics exist
			if (this.metricValues[serverId] != null) {
				throw new RuntimeException(String.format(
						"Something worng. Metrics for transaction %d on server %d has been added.",
						txNum, serverId));
			}
			
			// Check if there has been a master
			if (isMaster) {
				if (masterId != -1) {
					throw new RuntimeException(String.format(
							"Two machines declare itself as masters: %d, %d",
							masterId, serverId));
				}
				this.masterId = serverId;
			}
			
			this.metricValues[serverId] = metricValues;
		}

		@Override
		public String getVal(int index) {
			if (index == 0) {
				return Long.toString(txNum);
			} else if (index == 1) {
				return Integer.toString(masterId);
			} else {
				int metricCount = recorder.getMetricNameCount();
				int serverId = (index - 2) / metricCount;
				int metricPos = (index - 2) % metricCount;
				
				if (metricValues[serverId] != null &&
						metricValues[serverId].length > metricPos &&
						metricValues[serverId][metricPos] != null) {
					return metricValues[serverId][metricPos].toString();
				} else {
					return "";
				}
			}
		}

		@Override
		public int compareTo(MetricRow row) {
			return Long.compare(txNum, row.txNum);
		}
	}
	
	private AtomicBoolean isRecording = new AtomicBoolean(false);
	private BlockingQueue<TransactionMetrics> queue
		= new ArrayBlockingQueue<TransactionMetrics>(100000);
	
	// Data
	private List<MetricRow> rows = new ArrayList<MetricRow>();
	private Map<Long, Integer> txToRowPos = new HashMap<Long, Integer>();
	
	// Header
	private List<String> metricNames = new ArrayList<String>();
	private Map<String, Integer> metricNameToPos = new HashMap<String, Integer>();
	
	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			VanillaDb.taskMgr().runTask(this);
		}
	}
	
	public void addTransactionMetrics(TransactionMetrics metrics) {
		if (!isRecording.get())
			return;
		
		queue.add(metrics);
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
			addMetricsToRow(metrics);
			
			// Wait until no more metrics coming in the last 10 seconds
			while ((metrics = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				addMetricsToRow(metrics);
			}
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more metrics coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}
			
			// Sort by transaction ID
			Collections.sort(rows);
			
			// Save to CSV
			CsvSaver<MetricRow> csvSaver = new CsvSaver<MetricRow>(FILENAME_PREFIX);
			
			// Generate the output file
			List<String> header = generateHeader();
			csvSaver.generateOutputFile(header, rows);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private int addNewRow(long txNum) {
		rows.add(new MetricRow(this, txNum));
		return rows.size() - 1;
	}
	
	private void updateMissingMetrics(TransactionMetrics metrics) {
		for (String metricName : metrics.getMetrics().keySet()) {
			if (!metricNameToPos.containsKey(metricName)) {
				metricNames.add(metricName);
				metricNameToPos.put(metricName, metricNames.size() - 1);
			}
		}
	}
	
	private Object[] convertMetricsToArray(TransactionMetrics metrics) {
		updateMissingMetrics(metrics);
		
		Object[] metricValues = new Object[metricNames.size()];
		for (Map.Entry<String, Object> metricPair : metrics.getMetrics().entrySet()) {
			int pos = metricNameToPos.get(metricPair.getKey());
			metricValues[pos] = metricPair.getValue();
		}
		
		return metricValues;
	}
	
	private void addMetricsToRow(TransactionMetrics metrics) {
		Integer rowPos = txToRowPos.get(metrics.getTxNum());
		
		if (rowPos == null) {
			rowPos = addNewRow(metrics.getTxNum());
			txToRowPos.put(metrics.getTxNum(), rowPos);
		}
		
		MetricRow row = rows.get(rowPos);
		Object[] metricValues = convertMetricsToArray(metrics);
		boolean isMaster = metrics.getRole().equals("Master");
		row.addMetricValueForServer(metrics.getServerId(), isMaster, metricValues);
	}
	
	private int getMetricNameCount() {
		return metricNames.size();
	}
	
	private List<String> generateHeader() {
		List<String> header = new ArrayList<String>();
		
		// First column: transaction ID
		header.add(TRANSACTION_ID_COLUMN);
		
		// Second column: master node ID
		header.add(MASTER_ID_COLUMN);
		
		// After: metrics for each node
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			for (String metricName : metricNames)
				header.add(String.format("Server %d - %s", nodeId, metricName));
		}
		
		return header;
	}
}
