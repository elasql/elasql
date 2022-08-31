package org.elasql.perf.tpart.mdp.bandit.data;

import org.apache.commons.math3.linear.RealVectorFormat;
import org.elasql.perf.tpart.workload.TransactionFeaturesRecorder;
import org.elasql.server.Elasql;
import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;
import org.vanilladb.core.server.task.Task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A recorder to save BanditTransactionData objects to a CSV file.
 *
 * @author Yu-Xuan Lin, Yu-Shan Lin, Ping-Yu Wang, Yi-Sia Gao
 */
public class BanditTransactionDataRecorder extends Task {
	private static final String FILENAME_PREFIX = "bandit-transaction-data";
	private static final long TIME_TO_FLUSH = 10; // in seconds
	private static final Logger logger = Logger.getLogger(TransactionFeaturesRecorder.class.getName());
	private final AtomicBoolean isRecording = new AtomicBoolean(false);
	private final BlockingQueue<BanditDataRow> queue = new LinkedBlockingQueue<>();

	public void startRecording() {
		if (!isRecording.getAndSet(true)) {
			// Note: this should be called only once
			Elasql.taskMgr().runTask(this);
		}
	}

	public void record(BanditTransactionData banditTransactionData) {
		if (!isRecording.get()) return;

		BanditDataRow row = new BanditDataRow(banditTransactionData);
		queue.add(row);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Bandit Transaction Data Recorder");
		List<String> header = BanditDataRow.initHeader();
		int columnCount = header.size();

		try {
			// Wait for receiving the first statistics
			BanditDataRow row = queue.take();

			if (logger.isLoggable(Level.INFO)) logger.info("Bandit transaction data recorder starts");

			// Save to CSV
			CsvSaver<BanditDataRow> csvSaver = new CsvSaver<>(FILENAME_PREFIX);

			try (BufferedWriter writer = csvSaver.createOutputFile()) {
				csvSaver.writeHeader(writer, header);

				// write the first row
				csvSaver.writeRecord(writer, row, columnCount);

				// Wait until no more statistics coming in the last 10 seconds
				while ((row = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
					csvSaver.writeRecord(writer, row, columnCount);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("No more reward coming in last %d seconds. Start generating a report.", TIME_TO_FLUSH);
				logger.info(log);
			}

			if (logger.isLoggable(Level.INFO)) {
				String log = String.format("A reward log is generated at \"%s\"", csvSaver.fileName());
				logger.info(log);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	private static class BanditDataRow implements CsvRow, Comparable<BanditDataRow> {

		private final DecimalFormat formatter = new DecimalFormat("#.######");
		private final RealVectorFormat realVectorFormat = new RealVectorFormat("\"[", "]\"", ",");

		private final BanditTransactionData banditTransactionData;

		public BanditDataRow(BanditTransactionData banditTransactionData) {
			this.banditTransactionData = banditTransactionData;
		}

		static private List<String> initHeader() {
			List<String> header = new ArrayList<>();
			header.add("Transaction ID");
			header.add("Context");
			header.add("Arm");
			header.add("Reward");
			return header;
		}

		@Override
		public String getVal(int index) {
			switch (index) {
				case 0:
					return Long.toString(banditTransactionData.getTransactionNumber());
				case 1:
					return realVectorFormat.format(banditTransactionData.getContext());
				case 2:
					return Integer.toString(banditTransactionData.getArm());
				case 3:
					return formatter.format(banditTransactionData.getReward());
				default:
					throw new IllegalArgumentException("Invalid column index: " + index);
			}
		}

		@Override
		public int compareTo(BanditDataRow target) {
			return Long.compare(this.banditTransactionData.getTransactionNumber(), target.banditTransactionData.getTransactionNumber());
		}
	}
}
