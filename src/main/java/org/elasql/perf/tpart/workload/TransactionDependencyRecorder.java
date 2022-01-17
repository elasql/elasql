package org.elasql.perf.tpart.workload;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.vanilladb.core.server.task.Task;

/**
 * A recorder to save transaction dependencies to a CSV file.
 * 
 * @author Sheng-Yen Chou, Chao-Wei Lin, Yu-Xuan Lin, Yu-Shan Lin
 */
public class TransactionDependencyRecorder extends Task {
	private static Logger logger = Logger.getLogger(TransactionDependencyRecorder.class.getName());

	private static final String FILENAME = "transaction-dependencies.txt";
	private static final String TRANSACTION_ID_COLUMN = "Transaction ID";
	private static final String DEPENDENCY_COLUMN = "Dependent Transaction IDs";

	private static final long TIME_TO_FLUSH = 10; // in seconds

	private static class DependencyRow implements Comparable<DependencyRow> {
		Long txNum;
		Long[] dependencies;
		int index = 0;

		public DependencyRow(Long txNum, int length) {
			this.txNum = txNum;
			this.dependencies = new Long[length];
		}

		public void addValue(Long value) {
			dependencies[index] = value;
			index += 1;
		}

		@Override
		public int compareTo(DependencyRow target) {
			return txNum.compareTo(target.txNum);
		}
	}

	private AtomicBoolean isRecording = new AtomicBoolean(false);
	private BlockingQueue<DependencyRow> queue = new LinkedBlockingQueue<DependencyRow>();

	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			Elasql.taskMgr().runTask(this);
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

			saveToFile(row, queue);

			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more dependencies coming in last %d seconds. Start generating a report.",
						TIME_TO_FLUSH);
				logger.info(log);
			}

			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("A dependencies log is generated at \"%s\"", FILENAME);
				logger.info(log);
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void saveToFile(DependencyRow firstRow, BlockingQueue<DependencyRow> queue) throws InterruptedException {
		// Create a file writer
		try (PrintWriter writer = new PrintWriter(FILENAME)) {

			// Write the header
			writeHeader(writer);

			writeRow(writer, firstRow);

			DependencyRow row;

			// Wait until no more row coming in the last 10 seconds
			while ((row = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
				writeRow(writer, row);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeHeader(PrintWriter writer) {
		writer.format("%s => %s", TRANSACTION_ID_COLUMN, DEPENDENCY_COLUMN);
		writer.println(); // new line
	}

	private void writeRow(PrintWriter writer, DependencyRow row) {
		if (row.dependencies.length == 0) {
			writer.format("%d => X", row.txNum);
		} else if (row.dependencies.length >= 1) {
			// Write the first dependent transaction
			writer.format("%d => %d", row.txNum, row.dependencies[0]);

			// Add other dependent transactions
			for (int i = 1; i < row.dependencies.length; i++) {
				writer.format(", %d", row.dependencies[i]);
			}
		}
		writer.println(); // new line
	}
}
