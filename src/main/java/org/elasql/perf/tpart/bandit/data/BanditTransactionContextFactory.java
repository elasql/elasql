package org.elasql.perf.tpart.bandit.data;

import java.util.ArrayDeque;
import java.util.Queue;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class BanditTransactionContextFactory {

	private static final int WINDOW_SIZE = 100;
	
	private final int[] partitionLoad = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int txCount = 0;
	private Queue<Integer> loadHistory = new ArrayDeque<Integer>();

	public void addPartitionLoad(int partition) {
		txCount++;
		partitionLoad[partition]++;
		loadHistory.add(partition);
		if (loadHistory.size() > WINDOW_SIZE) {
			int removedPart = loadHistory.remove();
			partitionLoad[removedPart]--;
		}
	}

//	public void removePartitionLoad(int partition) {
//		txCount--;
//		partitionLoad[partition]--;
//	}

	public BanditTransactionContext buildContext(long txNum, TransactionFeatures transactionFeatures) {
		return new BanditTransactionContext(txNum, transactionFeatures, partitionLoad);
	}

	public double getPartitionLoad(int partition) {
		return txCount > 0 ? (double) partitionLoad[partition] / (double) txCount : 0;
	}
}
