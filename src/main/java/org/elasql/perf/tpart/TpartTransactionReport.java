package org.elasql.perf.tpart;

import org.elasql.perf.TransactionReport;

public class TpartTransactionReport implements TransactionReport {

	private static final long serialVersionUID = 20221024001L;
	
	private int masterId;
	private boolean isDistributed;
	private long latency;
	
	public TpartTransactionReport(int masterId, boolean isDistributed, long latency) {
		this.masterId = masterId;
		this.isDistributed = isDistributed;
		this.latency = latency;
	}
	
	public int getMasterId() {
		return masterId;
	}
	
	public boolean isDistributed() {
		return isDistributed;
	}
	
	public long getLatency() {
		return latency;
	}

}
