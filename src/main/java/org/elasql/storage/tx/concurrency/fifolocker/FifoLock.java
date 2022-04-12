package org.elasql.storage.tx.concurrency.fifolocker;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
	private AtomicReference<String> originalThreadName = new AtomicReference<String>();
	private AtomicBoolean isWaiting = new AtomicBoolean(false);

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
		
//		originalThreadName.set(Thread.currentThread().getName());
//		Thread.currentThread().setName(Thread.currentThread().getName() + " wait on " + key);
//		
//		System.out.println(originalThreadName + " waits on " + key);

		synchronized (this) {
			try {
				isWaiting.set(true);
				this.wait();
				isWaiting.set(false);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
//		
//		Thread.currentThread().setName(originalThreadName.get());
//		System.out.println(originalThreadName + " wakes on " + key);
	}

	public void notifyLock() {
//		if (!hasWaitedOnce.get()) {
//			throw new RuntimeException("Notifying a lock that is never wait can't be happening");
//		}
		synchronized (this) {
			this.notifyAll();
		}
	}
}