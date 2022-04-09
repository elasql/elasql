package org.elasql.storage.tx.concurrency;

public class FifoLock {
	public void waitOnLock() {
		synchronized(this) {
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