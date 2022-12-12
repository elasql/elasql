package org.elasql.perf.tpart;

import org.elasql.perf.TransactionReport;

public class TpartTransactionReport implements TransactionReport {

	private static final long serialVersionUID = 20221024001L;
	
	private int masterId;
	private boolean isDistributed;
	private int remoteReadCount;
	private long latency;
	
	public TpartTransactionReport(int masterId, boolean isDistributed, int remoteReadCount, long latency) {
		this.masterId = masterId;
		this.isDistributed = isDistributed;
		this.remoteReadCount = remoteReadCount;
		this.latency = latency;
	}
	
	public int getMasterId() {
		return masterId;
	}
	
	public boolean isDistributed() {
		return isDistributed;
	}
	
	public int getRemoteReadCount() {
		return remoteReadCount;
	}
	
	public long getLatency() {
		return latency;
	}

}
