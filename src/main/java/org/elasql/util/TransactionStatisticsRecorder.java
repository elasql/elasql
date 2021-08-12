package org.elasql.util;

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

import org.elasql.server.Elasql;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.Timer;

public class TransactionStatisticsRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionStatisticsRecorder.class.getName());

	// private static final String FILENAME_PREFIX =
	// "auto-bencher-workspace/results/transaction-statistics"+Elasql.serverId();
	private static final String FILENAME_PREFIX = "transaction-statistics";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	private static final String FIRST_STATS_COLUMN = "Execution Time";
	private static final long TIME_TO_FLUSH = 10; // in seconds
	private static final boolean ENABLED_STATISTIC_RECORDER = ElasqlProperties.getLoader()
			.getPropertyAsBoolean(TransactionStatisticsRecorder.class.getName() + ".ENABLED_STATISTIC_RECORDER", true);

	private static class StatisticRecord {
		String name;
		long time;

		public StatisticRecord(String name, long time) {
			this.name = name;
			this.time = time;
		}
	}

	private static class TransactionStatistics {
		long txNum;
		List<StatisticRecord> records = new ArrayList<StatisticRecord>();

		public TransactionStatistics(long txNum) {
			this.txNum = txNum;
		}

		public void addRecord(String name, long time) {
			records.add(new StatisticRecord(name, time));
		}
	}
	
	private static class StatisticsRow implements CsvRow, Comparable<StatisticsRow> {
		
		private long[] data;
		
		public StatisticsRow(long[] data) {
			this.data = data;
		}

		@Override
		public String getVal(int index) {
			if (index < data.length) {
				return Long.toString(data[index]);
			} else {
				return "0";
			}
		}

		@Override
		public int compareTo(StatisticsRow row) {
			return Long.compare(data[0], row.data[0]);
		}
	}
	
	private static AtomicBoolean isRecording = new AtomicBoolean(false);
	private static BlockingQueue<TransactionStatistics> queue = new ArrayBlockingQueue<TransactionStatistics>(100000);

	public static void startRecording() {
		// MODIFIED:
		if (ENABLED_STATISTIC_RECORDER) {
			if (!isRecording.getAndSet(true)) {
				// Note: this should be called only once
				VanillaDb.taskMgr().runTask(new TransactionStatisticsRecorder());
			}
		}
	}

	public static void recordResult(long txNum, Timer timer) {
		if (!isRecording.get())
			return;

		// MODIFIED: Print the timer
		TransactionStatistics stats = new TransactionStatistics(txNum);
		// System.out.print(timer.toString());
		for (Object component : timer.getComponents())
			stats.addRecord(component.toString(), timer.getComponentTime(component));
		queue.add(stats);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Statistics Recorder");

		try {
			// Initialize the header
			List<String> header = createInitialHeader();
			Map<String, Integer> columnToIndex = createHeaderToIndexMapping(header);

			// Wait for receiving the first statistics
			TransactionStatistics stats = queue.take();

			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction statistics recorder starts recording statistics");

			// Save the statistics
			List<StatisticsRow> rows = new ArrayList<StatisticsRow>();
			updateHeader(header, columnToIndex, stats);
			StatisticsRow row = convertStatisticsToRow(header, columnToIndex, stats);
			rows.add(row);
			
			// Wait until no more statistics coming in the last 10 seconds
			while ((stats = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				updateHeader(header, columnToIndex, stats);
				row = convertStatisticsToRow(header, columnToIndex, stats);
				rows.add(row);
			}

			// avgData = new long[6];

			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more statistics coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}

			// MODIFIED:
			// Generate output file of the transaction graph
			Elasql.getTransactionGraph().generateOutputFile();

			
			// Sort by transaction ID
			Collections.sort(rows);
			
			// Save to CSV
			CsvSaver<StatisticsRow> csvSaver = new CsvSaver<StatisticsRow>(FILENAME_PREFIX);
			
			// Generate the output file
			csvSaver.generateOutputFile(header, rows);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private List<String> createInitialHeader() {
		List<String> header = new ArrayList<String>();
		header.add(TRANSACTION_ID_COLUMN);
		header.add(FIRST_STATS_COLUMN);
		return header;
	}

	private Map<String, Integer> createHeaderToIndexMapping(List<String> header) {
		Map<String, Integer> columnToIndex = new HashMap<String, Integer>();
		for (int idx = 0; idx < header.size(); idx++)
			columnToIndex.put(header.get(idx), idx);
		return columnToIndex;
	}

	private void updateHeader(List<String> header, Map<String, Integer> columnToIndex, TransactionStatistics stats) {
		for (StatisticRecord record : stats.records) {
			// Depending on the size of the header, this may be slow.
			if (!columnToIndex.containsKey(record.name)) {
				header.add(record.name);
				columnToIndex.put(record.name, header.size() - 1);
			}
		}
	}
	
	private StatisticsRow convertStatisticsToRow(List<String> header, Map<String, Integer> columnToIndex, TransactionStatistics stats) {
		long[] data = new long[header.size()];
		data[0] = stats.txNum;
		for (StatisticRecord record : stats.records) {
			int index = columnToIndex.get(record.name);
			data[index] = record.time;
		}
		return new StatisticsRow(data);
	}
}
