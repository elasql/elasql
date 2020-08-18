package org.elasql.schedule.tpart.sink;

import org.elasql.sql.PrimaryKey;

public class PushInfo {
	private long destTxNum;
	private int serverId;
	private PrimaryKey record;

	public PushInfo(long destTxNum, int serverId, PrimaryKey record) {
		this.destTxNum = destTxNum;
		this.serverId = serverId;
		this.record = record;
	}

	public long getDestTxNum() {
		return destTxNum;
	}

	public void setDestTxNum(long destTxNum) {
		this.destTxNum = destTxNum;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public PrimaryKey getRecord() {
		return record;
	}

	public void setRecord(PrimaryKey record) {
		this.record = record;
	}

	public String toString() {
		return "{" + record + ":" + serverId + ":" + destTxNum + "}";
	}
}
