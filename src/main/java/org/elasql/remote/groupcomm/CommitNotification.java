package org.elasql.remote.groupcomm;

import java.io.Serializable;

public class CommitNotification implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long txNum;
	private int masterId;
	
	public CommitNotification(long txNum, int masterId) {
		this.txNum = txNum;
		this.masterId = masterId;
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public int getMasterId() {
		return masterId;
	}
}
