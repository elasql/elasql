package org.elasql.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class TransactionFeaturesRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionFeaturesRecorder.class.getName());
	
	private static final String FILENAME_PREFIX = "transaction-features";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	
	// Set 'true' to use the same filename for the report.
	// This is used to avoid create too many files in a series of experiments.
	// Because the file may be very large.
	private static final boolean USE_SAME_FILENAME = true;
	private static final long TIME_TO_FLUSH = 10; // in seconds
	
	private static class TransactionFeatures {
		Long txNum;
		Object[] values = new Object[FeatureCollector.keys.length];
		int index = 0;
		
		public TransactionFeatures(Long txNum) {
			this.txNum = txNum;
		}
		
		public void addValue(Object value) {
			values[index] = value;
			index += 1;
		}
		
		public Object[] getFeatrueList() {
			return values;
		}
	}
	
	private static AtomicBoolean isRecording = new AtomicBoolean(false);
	private static BlockingQueue<TransactionFeatures> queue
		= new ArrayBlockingQueue<TransactionFeatures>(100000);
	
	public static void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			VanillaDb.taskMgr().runTask(new TransactionFeaturesRecorder());
		}
	}
	
	public static void recordResult(long txNum, FeatureCollector featureCollector) {
		if (!isRecording.get())
			return;
			
		TransactionFeatures features = new TransactionFeatures(txNum);
		
		for (int i = 0;i<featureCollector.getKeyCount();i++)
			features.addValue(featureCollector.getFeatureValue(i));
		queue.add(features);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Features Recorder");
		
		try {
			// Wait for receiving the first statistics
			TransactionFeatures features = queue.take();
			
			// Initialize the header
			List<String> header = initHeader(features);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction features recorder starts");
			
			// Save the statistics
			List<TransactionFeatures> rows = new ArrayList<TransactionFeatures>();				
			rows.add(features);
			
			// Wait until no more statistics coming in the last 10 seconds
			while ((features = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				rows.add(features);
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

	private List<String> initHeader(TransactionFeatures features) {
		List<String> header = new ArrayList<String>();
		for (String key : FeatureCollector.keys) 
				header.add(key);
		return header;
	}

	private void generateOutputFile(List<String> header, List<TransactionFeatures> rows) {
		String fileName = generateOutputFileName();
		try (BufferedWriter writer = createOutputFile(fileName)) {
			sortByFirstColumn(rows);
			writeHeader(writer, header);
			for (TransactionFeatures row : rows)
				writeRecord(writer, row);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (logger.isLoggable(Level.INFO)) {
			String log = String.format("A transaction features file is generated at \"%s\"",
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
	
	// Sort by tx number
	private void sortByFirstColumn(List<TransactionFeatures> rows) {
		Collections.sort(rows, new Comparator<TransactionFeatures>() {

			@Override
			public int compare(TransactionFeatures row1, TransactionFeatures row2) {
				return row1.txNum.compareTo(row2.txNum);
			}
		});
	}
	
	private void writeHeader(BufferedWriter writer, List<String> header) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(TRANSACTION_ID_COLUMN);
		for (String column : header) {
			sb.append(',');
			sb.append(column);
		}
//		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}
	
	private void writeRecord(BufferedWriter writer, TransactionFeatures row) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(row.txNum);
		Object[] values = row.getFeatrueList();
		for (int i = 0; i < values.length; i++) {
			sb.append(',');
			sb.append(values[i]);		
		}
//		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}
}
