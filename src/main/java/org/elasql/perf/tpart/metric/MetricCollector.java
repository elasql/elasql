package org.elasql.perf.tpart.metric;

import org.elasql.server.Elasql;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.server.task.TaskMgr;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * A collector that collects system and transaction metrics on each machine.
 * 
 * @author Yu-Shan Lin
 */
public class MetricCollector extends Task {

	private static final int SYSTEM_METRIC_INTERVAL = 1000; // in milliseconds

	private TransactionMetricRecorder metricRecorder;

	public MetricCollector() {
		metricRecorder = new TransactionMetricRecorder(Elasql.serverId());
		metricRecorder.startRecording();
	}

	public void addTransactionMetrics(long txNum, String role, TransactionProfiler profiler) {
		metricRecorder.addTransactionMetrics(txNum, role, profiler);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("metric-collector");
		long startTime;

		try {
			while (true) {
				startTime = System.nanoTime();

				// Collect the metrics
				TPartSystemMetrics metrics = collectSystemMetrics();

				// Send to the sequencer
				if (Elasql.connectionMgr() != null)
					Elasql.connectionMgr().sendMetricReport(metrics);

				// Wait for the next collection
				while ((System.nanoTime() - startTime) / 1000_000 < SYSTEM_METRIC_INTERVAL) {
					Thread.sleep(SYSTEM_METRIC_INTERVAL / 10);
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private TPartSystemMetrics collectSystemMetrics() {
		TPartSystemMetrics.Builder builder = new TPartSystemMetrics.Builder(Elasql.serverId());

		// XXX: for demo
		builder.setFakeMetric(12345);
		builder.setThreadPoolSize(TaskMgr.THREAD_POOL_SIZE);
		
		
		return builder.build();
	}
}
