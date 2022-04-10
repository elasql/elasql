package org.elasql.storage.tx.concurrency.fifolocker;

/**
 * Transactions will wait on FifoLock on records behalf.
 * FifoLock also contains txNum information for {@link FifoLockers}
 * @author Pin-Yu Wang
 *
 */
public class FifoLock {
	private long txNum;

	public FifoLock(long txNum) {
		this.txNum = txNum;
	}

	public long getTxNum() {
		return txNum;
	}
	
	public boolean isMyFifoLock(long txNum) {
		return this.txNum == txNum;
	}

	public void waitOnLock() {
		synchronized (this) {
			try {
				this.wait();
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