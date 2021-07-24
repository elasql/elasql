package org.elasql.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.Timer;

public class TransactionStatisticsRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionStatisticsRecorder.class.getName());
	
	private static final String FILENAME_PREFIX = "transaction-statistics";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	private static final String FIRST_STATS_COLUMN = "Execution Time";
	
	// Set 'ture' to use the same filename for the report.
	// This is used to avoid create too many files in a series of experiments.
	private static final boolean USE_SAME_FILENAME = true;
	private static final long TIME_TO_FLUSH = 10; // in seconds

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
	
	private static AtomicBoolean isRecording = new AtomicBoolean(false);
	private static BlockingQueue<TransactionStatistics> queue
		= new ArrayBlockingQueue<TransactionStatistics>(100000);
	
	public static void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			VanillaDb.taskMgr().runTask(new TransactionStatisticsRecorder());
		}
	}
	
	public static void recordResult(long txNum, Timer timer) {
		if (!isRecording.get())
			return;
			
		TransactionStatistics stats = new TransactionStatistics(txNum);
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
			List<long[]> rows = new ArrayList<long[]>();
			updateHeader(header, columnToIndex, stats);
			long[] data = convertStatisticsToArray(header, columnToIndex, stats);
			rows.add(data);
			
			// Wait until no more statistics coming in the last 10 seconds
			while ((stats = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				updateHeader(header, columnToIndex, stats);
				data = convertStatisticsToArray(header, columnToIndex, stats);
				rows.add(data);
			}
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more statistics coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}
			
			// Generate the output file
			generateOutputFile(header, rows);
			
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
	
	private long[] convertStatisticsToArray(List<String> header, Map<String, Integer> columnToIndex, TransactionStatistics stats) {
		long[] data = new long[header.size()];
		data[0] = stats.txNum;
		for (StatisticRecord record : stats.records) {
			int index = columnToIndex.get(record.name);
			data[index] = record.time;
		}
		return data;
	}
	
	private void generateOutputFile(List<String> header, List<long[]> rows) {
		int columnCount = header.size();
		String fileName = generateOutputFileName();
		try (BufferedWriter writer = createOutputFile(fileName)) {
			sortByFirstColumn(rows);
			writeHeader(writer, header);
			for (long[] row : rows)
				writeRecord(writer, row, columnCount);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A transaction statistics report is generated at \"%s\"",
					fileName);
			logger.info(log);
		}
	}
	
	private String generateOutputFileName() {
		String filename;
		
		if (USE_SAME_FILENAME) {
			filename = String.format("%s.csv", FILENAME_PREFIX);
		} else {
			LocalDateTime datetime = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
			String datetimeStr = datetime.format(formatter);
			filename = String.format("%s-%s.csv", FILENAME_PREFIX, datetimeStr);
		}
		
		return filename;
	}
	
	private BufferedWriter createOutputFile(String fileName) throws IOException {
		return new BufferedWriter(new FileWriter(fileName));
	}
	
	private void sortByFirstColumn(List<long[]> rows) {
		Collections.sort(rows, new Comparator<long[]>() {

			@Override
			public int compare(long[] row1, long[] row2) {
				return Long.compare(row1[0], row2[0]);
			}
		});
	}
	
	private void writeHeader(BufferedWriter writer, List<String> header) throws IOException {
		StringBuilder sb = new StringBuilder();
		
		for (String column : header) {
			sb.append(column);
			sb.append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}
	
	private void writeRecord(BufferedWriter writer, long[] row, int columnCount) throws IOException {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < columnCount; i++) {
			if (i < row.length) {
				sb.append(row[i]);
				sb.append(',');
			} else {
				sb.append("0,");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}
}
