package org.elasql.perf.tpart.metric;

import org.elasql.server.Elasql;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * A collector that collects system and transaction metrics on
 * each machine. 
 * 
 * @author Yu-Shan Lin
 */
public class MetricCollector {
	
	private TransactionMetricRecorder metricRecorder;
	
	public MetricCollector() {
		metricRecorder = new TransactionMetricRecorder(Elasql.serverId());
		metricRecorder.startRecording();
	}
	
	public void addTransactionMetrics(long txNum, String role, TransactionProfiler profiler) {
		metricRecorder.addTransactionMetrics(txNum, role, profiler);
	}
}