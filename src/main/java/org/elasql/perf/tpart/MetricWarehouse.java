package org.elasql.perf.tpart;

import org.elasql.perf.tpart.ai.Estimator;

/**
 * A storage to record transaction metrics.
 * 
 * @author Yu-Shan Lin
 */
public class MetricWarehouse {
	
	private TransactionMetricRecorder metricRecorder;
	
	public MetricWarehouse() {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			metricRecorder = new TransactionMetricRecorder();
			metricRecorder.startRecording();
		}
	}
	
	public void receiveMetricReport(TPartMetricReport report) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			for (TransactionMetrics txMetrics : report.getTxMetrics()) {
				metricRecorder.addTransactionMetrics(txMetrics);
			}
		}
	}
	
}
