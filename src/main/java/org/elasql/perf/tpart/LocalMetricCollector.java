package org.elasql.perf.tpart;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.elasql.server.Elasql;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.Timer;

public class LocalMetricCollector extends Task {
	
	private static final int BATCH_SIZE = 1000;
	private static final int MAX_WAIT_TIME = 1000; // in milliseconds

	private BlockingQueue<TransactionMetrics> sendQueue;
	private int serverId;
	
	public LocalMetricCollector() {
		sendQueue = new LinkedBlockingQueue<TransactionMetrics>();
		serverId = Elasql.serverId();
	}
	
	public void addTransactionMetrics(long txNum, String role, Timer timer) {
		// Pull out the statistics in the timer
		Map<String, Object> metrics = new HashMap<String, Object>();
		for (Object component : timer.getComponents()) {
			metrics.put(component.toString(), timer.getComponentTime(component));
		}
		
		// Create a TransactionMetrics object
		TransactionMetrics txMetrics = new TransactionMetrics(txNum, serverId, role, metrics);
		
		// Save the object to the send queue
		sendQueue.add(txMetrics);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("local-metric-collector");
		
		try {
			List<TransactionMetrics> sendBatch = new ArrayList<TransactionMetrics>();
			long waitingStartTime = System.currentTimeMillis();
			
			while (true) {
				TransactionMetrics txMetrics = sendQueue.poll(MAX_WAIT_TIME / 10, TimeUnit.MILLISECONDS);
				
				// Add to the next batch
				if (txMetrics != null)
					sendBatch.add(txMetrics);
				
				// Check if this batch is big enough or it takes too much time
				if (sendBatch.size() < BATCH_SIZE &&
						System.currentTimeMillis() - waitingStartTime < MAX_WAIT_TIME) {
					continue;
				}
				
				// Send the batch to the sequencer
				if (!sendBatch.isEmpty()) {
					TPartMetricReport report = new TPartMetricReport(sendBatch);
					sendBatch = new ArrayList<TransactionMetrics>();
					Elasql.connectionMgr().sendMetricReport(report);
				}
				
				// Reset the time
				waitingStartTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
