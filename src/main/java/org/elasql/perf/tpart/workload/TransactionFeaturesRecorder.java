package org.elasql.perf.tpart.workload;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;
import org.vanilladb.core.server.task.Task;

/**
 * A recorder to save TransactionFeatures objects to a CSV file.
 * 
 * @author Yu-Xuan Lin, Yu-Shan Lin
 */
public class TransactionFeaturesRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionFeaturesRecorder.class.getName());
	
	private static final String FILENAME_PREFIX = "transaction-features";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	
	private static final long TIME_TO_FLUSH = 10; // in seconds
	
	private static class FeatureRow implements CsvRow, Comparable<FeatureRow> {
		
		private static DecimalFormat formatter = new DecimalFormat("#.######");
		Long txNum;
		Object[] values = new Object[TransactionFeatures.FEATURE_KEYS.size()];
		int index = 0;
		
		public FeatureRow(Long txNum) {
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
			else {
				Object val = values[index - 1];
				if (val.getClass().isArray())
					if (val instanceof Double[]) {
						int serverCount = PartitionMetaMgr.NUM_PARTITIONS;
						String[] StringVals = new String[serverCount];
						
						for (int serverId = 0; serverId < serverCount; serverId++)	
							StringVals[serverId] = formatter.format(((Double[]) val)[serverId]);
						
						return quoteString(Arrays.toString(StringVals));
					}
					else {
						return quoteString(Arrays.toString((Object[]) val));
					}
				else
					return val.toString();
			}
		}
		
		private String quoteString(String str)  {
			return "\"" + str + "\"";
		}

		@Override
		public int compareTo(FeatureRow target) {
			return txNum.compareTo(target.txNum);
		}
	}
	
	private AtomicBoolean isRecording = new AtomicBoolean(false);
	private BlockingQueue<FeatureRow> queue
		= new LinkedBlockingQueue<FeatureRow>();
	
	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			Elasql.taskMgr().runTask(this);
		}
	}
	
	public void record(TransactionFeatures features) {
		if (!isRecording.get())
			return;
			
		FeatureRow row = new FeatureRow(features.getTxNum());
		for (String key : TransactionFeatures.FEATURE_KEYS)
			row.addValue(features.getFeature(key));
		queue.add(row);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Features Recorder");
		
		try {
			// Wait for receiving the first statistics
			FeatureRow row = queue.take();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction features recorder starts");
			
			// Save the row
			List<FeatureRow> rows = new ArrayList<FeatureRow>();				
			rows.add(row);
			
			// Wait until no more statistics coming in the last 10 seconds
			while ((row = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				rows.add(row);
			}
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more features coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}
			
			// Sort by transaction ID
			Collections.sort(rows);
			
			// Save to CSV
			CsvSaver<FeatureRow> csvSaver = new CsvSaver<FeatureRow>(FILENAME_PREFIX);
			
			// Generate the output file
			List<String> header = initHeader();
			String fileName = csvSaver.generateOutputFile(header, rows);
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("A feature log is generated at \"%s\"", fileName);
				logger.info(log);
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private List<String> initHeader() {
		List<String> header = new ArrayList<String>();
		header.add(TRANSACTION_ID_COLUMN);
		for (String key : TransactionFeatures.FEATURE_KEYS) 
			header.add(key);
		return header;
	}
}
