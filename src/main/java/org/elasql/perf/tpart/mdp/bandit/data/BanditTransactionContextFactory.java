package org.elasql.perf.tpart.mdp.bandit.data;

import java.util.Arrays;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.elasql.perf.tpart.mdp.State;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class BanditTransactionContextFactory {
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;

	public BanditTransactionContext buildContext(long txNum, State state) {
		double[] localReadCounts = new double[NUM_PARTITIONS];
		double[] machineLoads = new double[NUM_PARTITIONS];
		
		for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
			localReadCounts[partId] = state.getLocalRead(partId);
			machineLoads[partId] = state.getMachineLoad(partId);
		}
		
		// Note: no need to normalize machineLoads
		normalize(localReadCounts);
		
		ArrayRealVector vector = new ArrayRealVector(localReadCounts, machineLoads);
		return new BanditTransactionContext(txNum, vector);
	}

	private void normalize(double[] array) {
		double sum = Arrays.stream(array).sum();
		if (sum == 0) {
			return;
		}
		for (int i = 0; i < array.length; i++) {
			array[i] /= sum;
		}
	}
}
