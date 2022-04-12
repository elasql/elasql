package org.elasql.storage.tx.concurrency.fifolocker;

import java.util.concurrent.atomic.AtomicBoolean;

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
	private AtomicBoolean sLockable = new AtomicBoolean(false);
	private AtomicBoolean xLockable = new AtomicBoolean(false);

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

	public void setSLockable() {
		sLockable.set(true);
	}

	public void setXLockable() {
		xLockable.set(true);
	}
	
	public boolean getSLockable() {
		return sLockable.get();
	}
	
	public boolean getXLockable() {
		return xLockable.get();
	}
	
	public void resetLockable() {
		sLockable.set(false);
		xLockable.set(false);
	}

	public boolean isMyFifoLock(FifoLock fifoLock) {
		return this == fifoLock;
	}

	public void waitOnSLock() {
		if (sLockable.get()) {
			return;
		}
		try {
			this.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void waitOnXLock() {
		if (xLockable.get() ) {
			return;
		}
		
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