package org.elasql.perf.tpart;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;
import org.vanilladb.core.server.task.Task;

public class TransactionStatisticsRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionStatisticsRecorder.class.getName());
	
	private static final long TIME_TO_FLUSH = 10; // in seconds
	private static final int WINDOW_SIZE = 5; // in seconds

	private static class StatisticsRow implements CsvRow {
		
		private int time;
		private int txCount;
		private double latencyAvg;
		private double latencyStd;
		private double distTxRate;
		private double avgRemoteReads;
		private double imbalanceScore;

		public StatisticsRow(int time, int txCount, double latencyAvg, double latencyStd, double distTxRate,
				double avgRemoteReads, double imbalanceScore) {
			this.time = time;
			this.txCount = txCount;
			this.latencyAvg = latencyAvg;
			this.latencyStd = latencyStd;
			this.distTxRate = distTxRate;
			this.avgRemoteReads = avgRemoteReads;
			this.imbalanceScore = imbalanceScore;
		}
		
		@Override
		public String getVal(int index) {
			switch (index) {
			case 0:
				return Integer.toString(time);
			case 1:
				return Integer.toString(txCount);
			case 2:
				return Double.toString(latencyAvg);
			case 3:
				return Double.toString(latencyStd);
			case 4:
				return Double.toString(distTxRate);
			case 5:
				return Double.toString(avgRemoteReads);
			case 6:
				return Double.toString(imbalanceScore);
			default:
				throw new IllegalArgumentException("No column with index " + index);
			}
		}
	}
	
	private BlockingQueue<TpartTransactionReport> queue = new LinkedBlockingQueue<TpartTransactionReport>();
	
	// Statistics
	private int distTxCount, remoteReadCount;
	private List<Long> latencies = new ArrayList<Long>();
	private int[] loads = new int[PartitionMetaMgr.NUM_PARTITIONS];
	
	public void onTansactionCommit(long txNum, TpartTransactionReport report) {
		queue.add(report);
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Features Recorder");
		List<String> header = initHeader();
		int columnCount = header.size();

		try {
			// Wait for receiving the first report
			TpartTransactionReport report = queue.take();
			long startTime = System.currentTimeMillis();
			long nextWriteTime = WINDOW_SIZE * 1000;
			processReport(report);

			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction statistics recorder starts");

			// Create a CSV file
			CsvSaver<StatisticsRow> csvSaver = new CsvSaver<StatisticsRow>("transaction-statistics", false);

			try (BufferedWriter writer = csvSaver.createOutputFile()) {
				csvSaver.writeHeader(writer, header);

				// Wait until no more statistics coming in the last 10 seconds
				while ((report = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
					// Calculate the statistics in the current window
					long currentTime = System.currentTimeMillis() - startTime;
					if (currentTime >= nextWriteTime) {
						// Write to the CSV file
						StatisticsRow row = calculateStatistics(nextWriteTime / 1000);
						csvSaver.writeRecord(writer, row, columnCount);
						writer.flush();
						
						nextWriteTime += WINDOW_SIZE * 1000;
					}
					
					// Process the report 'after' the statistics calculation
					// since this report belongs to the next window
					processReport(report);
				}

				if (logger.isLoggable(Level.INFO)) {
					String log = String.format("No more transaction coming in last %d seconds. Start generating a report.",
							TIME_TO_FLUSH);
					logger.info(log);
				}
				
				// Calculate the statistics in the last window
				StatisticsRow row = calculateStatistics(nextWriteTime / 1000);
				csvSaver.writeRecord(writer, row, columnCount);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("A statistic report is generated at \"%s\"", csvSaver.fileName());
				logger.info(log);
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private List<String> initHeader() {
		List<String> header = new ArrayList<String>();
		header.add("End Time (second)");
		header.add("Throughput");
		header.add("Average Latency (microseconds)");
		header.add("Latency STD (microseconds)");
		header.add("Distributed Txn. Rate");
		header.add("Average Number of Remote Reads");
		header.add("Imbalanced Score");
		return header;
	}
	
	private void processReport(TpartTransactionReport report) {
		latencies.add(report.getLatency());
		if (report.isDistributed())
			distTxCount++;
		remoteReadCount += report.getRemoteReadCount();
		loads[report.getMasterId()]++;
	}
	
	private StatisticsRow calculateStatistics(long timeInSec) {
		// Calculate the statistics
		int txCount = latencies.size();
		double distTxRate = ((double) distTxCount) / txCount;
		double latencyAvg = calculateLatencyAvg();
		double latencyStd = calculateLatencyStd(latencyAvg);
		double avgRemoteReads = ((double) remoteReadCount) / txCount;
		double imbalanceScore = calculateImbalanceScore();
		
		// Reset the values
		latencies.clear();
		distTxCount = 0;
		remoteReadCount = 0;
		
		return new StatisticsRow((int) timeInSec, txCount, latencyAvg, latencyStd,
				distTxRate, avgRemoteReads, imbalanceScore);
	}
	
	private double calculateLatencyAvg() {
		double avg = 0.0;
		double txCount = latencies.size();
		for (Long latency : latencies) {
			avg += latency.doubleValue() / txCount;
		}
		return avg;
	}
	
	private double calculateLatencyStd(double latencyAvg) {
		double var = 0.0;
		double txCount = latencies.size();
		for (Long latency : latencies) {
			var += Math.pow(latency.doubleValue() - latencyAvg, 2) / txCount;
		}
		return Math.sqrt(var);
	}
	
	private double calculateImbalanceScore() {
		// Average load
		double avg = 0.0;
		for (int i = 0; i < loads.length; i++) {
			avg += loads[i];
		}
		avg /= loads.length;
		
		// Score
		double score = 0.0;
		for (int i = 0; i < loads.length; i++) {
			score += Math.abs(1 - loads[i] / avg);
		}
		
		return score;
	}
}
