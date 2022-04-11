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
	private long txNum;
	private String originalThreadName;

	public FifoLock(long txNum) {
		this.txNum = txNum;
	}

	public long getTxNum() {
		return txNum;
	}

	public boolean isMyFifoLock(long txNum) {
		return this.txNum == txNum;
	}

	public void waitOnLock(Object obj) {
		synchronized (this) {
			try {
				originalThreadName = Thread.currentThread().getName();
				Thread.currentThread().setName(originalThreadName + " wait on " + (PrimaryKey)obj);
				this.wait();
				Thread.currentThread().setName(originalThreadName);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void notifyLock() {
		synchronized (this) {
			this.notify();
		}
	}
}