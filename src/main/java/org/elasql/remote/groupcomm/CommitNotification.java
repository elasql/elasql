package org.elasql.remote.groupcomm;

import java.io.Serializable;

public class CommitNotification implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long txNum;
	private int masterId;
	private long txLatency;
	
	public CommitNotification(long txNum, int masterId, long txLatency) {
		this.txNum = txNum;
		this.masterId = masterId;
		this.txLatency = txLatency;
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public int getMasterId() {
		return masterId;
	}
	
	public long getTxLatency() {
		return txLatency;
	}
}
