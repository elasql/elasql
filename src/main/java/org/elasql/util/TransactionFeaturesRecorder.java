package org.elasql.util;

import java.util.ArrayList;
import java.util.Collections;
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
	
	private static final long TIME_TO_FLUSH = 10; // in seconds
	
	private static class TransactionFeatures implements CsvRow, Comparable<TransactionFeatures> {
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

		@Override
		public String getVal(int index) {
			if (index == 0)
				return Long.toString(txNum);
			else
				return values[index - 1].toString();
		}

		@Override
		public int compareTo(TransactionFeatures target) {
			return txNum.compareTo(target.txNum);
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
			
			// Sort by transaction ID
			Collections.sort(rows);
			
			// Save to CSV
			CsvSaver<TransactionFeatures> csvSaver = new CsvSaver<TransactionFeatures>(FILENAME_PREFIX);
			
			// Generate the output file
			csvSaver.generateOutputFile(header, rows);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private List<String> initHeader(TransactionFeatures features) {
		List<String> header = new ArrayList<String>();
		header.add(TRANSACTION_ID_COLUMN);
		for (String key : FeatureCollector.keys) 
				header.add(key);
		return header;
	}
}
