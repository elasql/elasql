package org.elasql.remote.groupcomm;

import java.io.Serializable;

import org.elasql.perf.TransactionReport;

public class CommitNotification implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long txNum;
	private TransactionReport report;
	
	public CommitNotification(long txNum, TransactionReport report) {
		this.txNum = txNum;
		this.report = report;
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public TransactionReport getReport() {
		return report;
	}
}
