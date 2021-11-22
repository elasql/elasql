package org.elasql.perf.tpart.workload;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.PrimaryKey;

/**
 * An analyzer that records how each transaction uses records
 * and find out the dependent transactions for each transaction.
 * 
 * @author Sheng-Yen Chou, Chao-Wei Lin, Yu-Shan Lin
 *
 */
public class TransactionDependencyAnalyzer {
	
	/**
	 * This records who uses the key most recently.
	 */
	private static class LastUse {
		Long lastWriteTx = -1l;
		Set<Long> lastReadTxs = new HashSet<Long>();
		
		void addNewReadTx(long readTx) {
			lastReadTxs.add(readTx);
		}
		
		void setNewWriteTx(long writeTx) {
			lastWriteTx = writeTx;
			// When a new write tx appears,
			// the following read/write tx must depend
			// on this new tx.
			// So, it is no need to record which txs read.
			lastReadTxs.clear();
		}
	}
	
	private Map<PrimaryKey, LastUse> lastUses = new HashMap<PrimaryKey, LastUse>();
	
	public Set<Long> addAndGetDependency(long txNum, Set<PrimaryKey> readSet, Set<PrimaryKey> writeSet) {
		// Find the dependent transactions
		Set<Long> dependentTxs = findDependentTransactions(readSet, writeSet);
		
		// Add this transaction to owners
		addTransaction(txNum, readSet, writeSet);
		
		return dependentTxs;
	}
	
	private Set<Long> findDependentTransactions(Set<PrimaryKey> readSet, Set<PrimaryKey> writeSet) {
		Set<Long> dependentTxs = new HashSet<Long>();
		
		// Write-Read dependencies
		for (PrimaryKey readKey : readSet) {
			if (!writeSet.contains(readKey)) {
				LastUse lastUse = lastUses.get(readKey);
				if (lastUse != null && lastUse.lastWriteTx != -1l) {
					dependentTxs.add(lastUse.lastWriteTx);
				}
			}
		}
		
		// Read-Write/Write-Write dependencies
		for (PrimaryKey writeKey : writeSet) {
			LastUse lastUse = lastUses.get(writeKey);
			if (lastUse != null) {
				// Check if there is any previous reads.
				// If the lastReadTxs is empty, it means
				// that the latest tx must be a write tx.
				if (!lastUse.lastReadTxs.isEmpty()) {
					for (Long readTx : lastUse.lastReadTxs)
						dependentTxs.add(readTx);
				} else {
					dependentTxs.add(lastUse.lastWriteTx);
				}
			}
		}
		
		return dependentTxs;
	}
	
	private LastUse getLastUseForUpdate(PrimaryKey key) {
		LastUse lastUse = lastUses.get(key);
		
		if (lastUse == null) {
			lastUse = new LastUse();
			lastUses.put(key, lastUse);
		}
		
		return lastUse;
	}
	
	private void addTransaction(long txNum, Set<PrimaryKey> readSet, Set<PrimaryKey> writeSet) {
		for (PrimaryKey readKey : readSet) {
			if (!writeSet.contains(readKey)) {
				LastUse lastUse = getLastUseForUpdate(readKey);
				lastUse.addNewReadTx(txNum);
			}
		}

		for (PrimaryKey writeKey : writeSet) {
			LastUse lastUse = getLastUseForUpdate(writeKey);
			lastUse.setNewWriteTx(txNum);
		}
	}
}
