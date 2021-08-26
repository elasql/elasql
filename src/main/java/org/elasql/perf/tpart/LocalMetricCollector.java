package org.elasql.perf.tpart;

import org.elasql.server.Elasql;
import org.vanilladb.core.util.Timer;

public class LocalMetricCollector {
	
	private TransactionMetricRecorder metricRecorder;
	
	public LocalMetricCollector() {
		metricRecorder = new TransactionMetricRecorder(Elasql.serverId());
		metricRecorder.startRecording();
	}
	
	public void addTransactionMetrics(long txNum, String role, Timer timer) {
		metricRecorder.addTransactionMetrics(txNum, role, timer);
	}
}
