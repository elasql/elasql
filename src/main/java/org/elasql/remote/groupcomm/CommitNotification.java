package org.elasql.remote.groupcomm;

import java.io.Serializable;

public class CommitNotification implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long txNum;
	
	public CommitNotification(long txNum) {
		this.txNum = txNum;
	}
	
	public long getTxNum() {
		return txNum;
	}

}
