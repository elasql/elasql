package org.elasql.storage.tx.concurrency;

/**
 * Transactions will wait on FifoLock on records behalf. FifoLock also contains
 * txNum information for {@link FifoLockers}
 * 
 * @author Pin-Yu Wang, Yu-Shan Lin
 *
 */
public class FifoLock {
	private final long txNum;
	
	public FifoLock(long txNum) {
		this.txNum = txNum;
	}

	long getTxNum() {
		return txNum;
	}

	boolean isMyFifoLock(FifoLock fifoLock) {
		return this == fifoLock;
	}

	void waitOnLock() {
		try {
			this.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	void notifyLock() {
		this.notify();
	}
}