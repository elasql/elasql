package org.elasql.perf.tpart.bandit.data;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class BanditTransactionContextFactory {
	private final int[] partitionLoad = new int[PartitionMetaMgr.NUM_PARTITIONS];

	public void addPartitionLoad(int partition) {
		partitionLoad[partition]++;
	}

	public void removePartitionLoad(int partition) {
		partitionLoad[partition]--;
	}

	public BanditTransactionContext buildContext(long txNum, TransactionFeatures transactionFeatures) {
		return new BanditTransactionContext(txNum, transactionFeatures, partitionLoad);
	}
}
