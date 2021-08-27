package org.elasql.perf.tpart.workload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

/**
 * A recorder to save transaction dependencies to a CSV file.
 * 
 * @author Sheng-Yen Chou, Chao-Wei Lin, Yu-Xuan Lin, Yu-Shan Lin
 */
public class TransactionDependencyRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionDependencyRecorder.class.getName());
	
	private static final String FILENAME_PREFIX = "transaction-dependencies";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	
	private static final long TIME_TO_FLUSH = 10; // in seconds
	
	private static class DependencyRow implements CsvRow, Comparable<DependencyRow> {
		Long txNum;
		Object[] dependencies;
		int index = 0;
		
		public DependencyRow(Long txNum, int length) {
			this.txNum = txNum;
			this.dependencies = new Object[length];
		}
		
		public void addValue(Object value) {
			dependencies[index] = value;
			index += 1;
		}
		
		public int getColumnCount() {
			return dependencies.length + 1;
		}

		@Override
		public String getVal(int index) {
			if (index == 0)
				return Long.toString(txNum);
			else if (index > dependencies.length)
				return "";
			else
				return dependencies[index - 1].toString();
		}

		@Override
		public int compareTo(DependencyRow target) {
			return txNum.compareTo(target.txNum);
		}
	}
	
	private AtomicBoolean isRecording = new AtomicBoolean(false);
	private BlockingQueue<DependencyRow> queue
		= new ArrayBlockingQueue<DependencyRow>(100000);
	
	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			VanillaDb.taskMgr().runTask(this);
		}
	}
	
	public void record(TransactionFeatures features) {
		if (!isRecording.get())
			return;
			
		List<Long> dependencies = features.getDependencies();
		DependencyRow row = new DependencyRow(features.getTxNum(), dependencies.size());
		for (Long dependency : dependencies)
			row.addValue(dependency);
		queue.add(row);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Transaction Dependencies Recorder");
		
		try {
			// Wait for receiving the first statistics
			DependencyRow row = queue.take();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Transaction dependencies recorder starts");
			
			// Save the row
			List<DependencyRow> rows = new ArrayList<DependencyRow>();				
			rows.add(row);
			
			// Wait until no more row coming in the last 10 seconds
			while ((row = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				rows.add(row);
			}
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more dependencies coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}
			
			// Sort by transaction ID
			Collections.sort(rows);
			
			// Count the number of columns
			int columnCount = 1;
			for (DependencyRow r : rows)
				if (r.getColumnCount() > columnCount)
					columnCount = r.getColumnCount();
			
			// Save to CSV
			CsvSaver<DependencyRow> csvSaver = new CsvSaver<DependencyRow>(FILENAME_PREFIX);
			
			// Generate the output file
			List<String> header = initHeader(columnCount);
			String fileName = csvSaver.generateOutputFile(header, rows);
			
			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("A dependencies log is generated at \"%s\"", fileName);
				logger.info(log);
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private List<String> initHeader(int columnCount) {
		List<String> header = new ArrayList<String>();
		header.add(TRANSACTION_ID_COLUMN);
		for (int i = 1; i < columnCount; i++)
			header.add(Integer.toString(i));
		return header;
	}
}
