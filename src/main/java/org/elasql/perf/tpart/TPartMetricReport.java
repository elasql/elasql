package org.elasql.perf.tpart;

import java.util.List;

import org.elasql.perf.MetricReport;

public class TPartMetricReport implements MetricReport {

	private static final long serialVersionUID = 20210824001L;
	
	private List<TransactionMetrics> txMetrics;
	
	public TPartMetricReport(List<TransactionMetrics> txMetrics) {
		this.txMetrics = txMetrics;
	}
	
	public List<TransactionMetrics> getTxMetrics() {
		return txMetrics;
	}
}
