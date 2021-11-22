package org.elasql.perf.tpart.ai;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.elasql.storage.metadata.PartitionMetaMgr;

/**
 * A set of estimation for a particular transaction.<br>
 * <br>
 * Note that we convert the instance of this class by manually
 * serialize it to bytes when we send it through the network.
 * This is because we found that the default serialization
 * creates a much bigger message than we thought it would be.
 */
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
	
	public static TransactionEstimation fromBytes(byte[] bytes)
			throws IOException {
		double[] latencies = new double[PartitionMetaMgr.NUM_PARTITIONS];
		long[] masterCpus = new long[PartitionMetaMgr.NUM_PARTITIONS];
		long[] slaveCpus = new long[PartitionMetaMgr.NUM_PARTITIONS];
		
		ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
		ObjectInputStream in = new ObjectInputStream(bi);
		
		for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++) {
			latencies[i] = in.readDouble();
			masterCpus[i] = in.readLong();
			slaveCpus[i] = in.readLong();
		}
		
		return new TransactionEstimation(latencies, masterCpus, slaveCpus);
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
	
	public byte[] toBytes() {
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(bo);
			
			for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++) {
				out.writeDouble(latencies[i]);
				out.writeLong(masterCpus[i]);
				out.writeLong(slaveCpus[i]);
			}
			
			out.flush();
			
			return bo.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
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
