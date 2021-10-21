package org.elasql.perf.tpart.ai;

import java.io.Serializable;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class TransactionEstimation implements Serializable {
	
	private static final long serialVersionUID = 20210916001L;
	
	public static class Builder {
		private double[] latencies;
		private long[] masterCpus;
		private long[] slaveCpus;
		
		public Builder() {
			latencies = new double[PartitionMetaMgr.NUM_PARTITIONS];
			masterCpus = new long[PartitionMetaMgr.NUM_PARTITIONS];
			slaveCpus = new long[PartitionMetaMgr.NUM_PARTITIONS];
		}
		
		public void setLatency(int masterId, double latency) {
			this.latencies[masterId] = latency;
		}
		
		public void setMasterCpuCost(int masterId, long cpuCost) {
			this.masterCpus[masterId] = cpuCost;
		}
		
		public void setSlaveCpuCost(int slaveId, long cpuCost) {
			this.slaveCpus[slaveId] = cpuCost;
		}
		
		public TransactionEstimation build() {
			return new TransactionEstimation(latencies, masterCpus, slaveCpus);
		}
	}

	private double[] latencies;
	private long[] masterCpus;
	private long[] slaveCpus;
	
	// TODO: add disk and network costs
	
	private TransactionEstimation(double[] latencies, long[] masterCpus,
			long[] slaveCpus) {
		this.latencies = latencies;
		this.masterCpus = masterCpus;
		this.slaveCpus = slaveCpus;
	}
	
	public double estimateLatency(int masterId) {
		return latencies[masterId];
	}
	
	public long estimateMasterCpuCost(int masterId) {
		return masterCpus[masterId];
	}
	
	public long estimateSlaveCpuCost(int slaveId) {
		return slaveCpus[slaveId];
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("{latency:[");
		for (double latency : latencies) 
			sb.append(String.format("%f,", latency));
		sb.deleteCharAt(sb.length() - 1);
		sb.append("], masterCpus:[");
		for (long masterCpu : masterCpus) 
			sb.append(String.format("%d,", masterCpu));
		sb.deleteCharAt(sb.length() - 1);
		sb.append("], slaveCpus:[");
		for (long slaveCpu : slaveCpus) 
			sb.append(String.format("%d,", slaveCpu));
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]}");

		return sb.toString();
	}
}
