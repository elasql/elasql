package org.elasql.perf.tpart.bandit.data;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class BanditTransactionContextFactory {
	private final int[] partitionLoad = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int txCount = 0;

	public void addPartitionLoad(int partition) {
		txCount++;
		partitionLoad[partition]++;
	}

	public void removePartitionLoad(int partition) {
		txCount--;
		partitionLoad[partition]--;
	}

	public BanditTransactionContext buildContext(long txNum, TransactionFeatures transactionFeatures) {
		return new BanditTransactionContext(txNum, transactionFeatures, partitionLoad);
	}

	public double getPartitionLoad(int partition) {
		return txCount > 0 ? (double) partitionLoad[partition] / (double) txCount : 0;
	}
}
