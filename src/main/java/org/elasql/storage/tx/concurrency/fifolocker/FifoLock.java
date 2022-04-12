package org.elasql.storage.tx.concurrency.fifolocker;

import org.elasql.sql.PrimaryKey;

/**
 * Transactions will wait on FifoLock on records behalf. FifoLock also contains
 * txNum information for {@link FifoLockers}
 * 
 * @author Pin-Yu Wang
 *
 */
public class FifoLock {
	private final PrimaryKey key;
	private final long txNum;

	public FifoLock(PrimaryKey key, long txNum) {
		this.key = key;
		this.txNum = txNum;
	}

	public long getTxNum() {
		return txNum;
	}

	public PrimaryKey getKey() {
		return key;
	}

	public boolean isMyFifoLock(FifoLock fifoLock) {
		return this == fifoLock;
	}

	public void waitOnLock() {
		try {
			this.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void notifyLock() {
		this.notify();
	}
}