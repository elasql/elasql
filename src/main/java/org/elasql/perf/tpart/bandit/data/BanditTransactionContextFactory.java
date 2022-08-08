package org.elasql.perf.tpart.bandit.data;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

import java.util.ArrayDeque;

public class BanditTransactionContextFactory {
	private static final int MACHINE_LOAD_WINDOW_SIZE;

	static {
		MACHINE_LOAD_WINDOW_SIZE = ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditTransactionContextFactory.class.getName() + ".MACHINE_LOAD_WINDOW_SIZE", 200);
	}

	private final int[] partitionLoad = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private final ArrayDeque<Integer> window = new ArrayDeque<>(MACHINE_LOAD_WINDOW_SIZE);

	public void addArm(int arm) {
		if (window.size() >= MACHINE_LOAD_WINDOW_SIZE) {
			partitionLoad[window.remove()]--;
		}
		window.add(arm);
		partitionLoad[arm]++;
	}

	public BanditTransactionContext buildContext(long txNum, TransactionFeatures transactionFeatures) {
		return new BanditTransactionContext(txNum, transactionFeatures, partitionLoad);
	}

	public double getPartitionLoad(int partition) {
		int size = window.size();
		return size > 0 ? (double) partitionLoad[partition] / (double) size : 0;
	}
}
