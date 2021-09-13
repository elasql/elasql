package org.elasql.perf.tpart.ai;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class TransactionEstimation {
	
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
}
