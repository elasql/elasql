package org.elasql.perf.tpart;

import java.io.Serializable;
import java.util.Map;

public class TransactionMetrics implements Serializable {

	private static final long serialVersionUID = 20210823001L;
	
	private long txNum;
	private int serverId;
	private String role;
	private Map<String, Object> metrics;
	
	TransactionMetrics(long txNum, int serverId, String role, Map<String, Object> metrics) {
		this.txNum = txNum;
		this.serverId = serverId;
		this.role = role;
		this.metrics = metrics;
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public int getServerId() {
		return serverId;
	}
	
	public String getRole() {
		return role;
	}
	
	public Map<String, Object> getMetrics() {
		return metrics;
	}
}
