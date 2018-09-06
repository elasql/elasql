/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.storage.tx.concurrency.tpart;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class LocalStorageLockTable {

	private static final int NUM_ANCHOR = 1009;
	
	public enum LockType {
		S_LOCK, X_LOCK
	}

	private class Lockers {
		static final long NONE = -1; // for sixLocker, xLocker and wbLocker
		
		List<Long> sLockers;
		// only one tx can hold xLock(sixLock) on single item
		long xLocker;
		Queue<Long> requestQueue;

		Lockers() {
			sLockers = new LinkedList<Long>();
			xLocker = NONE;
			requestQueue = new LinkedList<Long>();
		}
		
		@Override
		public String toString() {
			return "{S: " + sLockers +
					", X: " + xLocker +
					", requests: " + requestQueue +
					"}";
		}
	}

	private Map<Object, Lockers> lockerMap = new ConcurrentHashMap<Object, Lockers>();

	// Lock-stripping
	private final Object anchors[] = new Object[NUM_ANCHOR];

	/**
	 * Create and initialize a conservative ordered lock table.
	 */
	public LocalStorageLockTable() {
		// Initialize anchors
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	/**
	 * Request lock for an object. This method will put the requested
	 * transaction into a waiting queue of requested object.
	 * 
	 * @param obj
	 *            the object which transaction request lock for
	 * @param txNum
	 *            the transaction that requests the lock
	 */
	public void requestLock(Object obj, long txNum) {
		System.out.println("Tx." + txNum + " request lock on " + obj);
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);
			lockers.requestQueue.add(txNum);
		}
	}

	/**
	 * Grants an slock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	public void sLock(Object obj, long txNum) {
		Object anchor = getAnchor(obj);

		synchronized (anchor) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already held the lock
			if (hasSLock(lockers, txNum)) {
				lockers.requestQueue.remove(txNum);
				return;
			}

			try {
				String name = Thread.currentThread().getName();
				
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.requestQueue.peek();
				while (!sLockable(lockers, txNum) || (head != null && head.longValue() != txNum)) {
					
					long target = lockers.xLocker;
					if (target == -1)
						target = head;
					Thread.currentThread().setName(name + " waits for slock of " + obj + " from tx." + target);
//					
					anchor.wait();

					// Since a transaction may delete the lockers of an object
					// after releasing them, it should call prepareLockers()
					// here, instead of using lockers it obtains earlier.
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				Thread.currentThread().setName(name);
				
				if (!sLockable(lockers, txNum))
					throw new LockAbortException();

				// get the s lock
				lockers.requestQueue.poll();
				lockers.sLockers.add(txNum);

				// Wake up other waiting transactions (on this object) to let
				// them
				// fight for the lockers on this object.
				anchor.notifyAll();
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new LockAbortException("Interrupted when waitting for lock");
			}
		}
		System.out.println("Tx." + txNum + " get slock of " + obj);
	}

	/**
	 * Grants an xlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	public void xLock(Object obj, long txNum) {
		// See the comments in sLock(..) for the explanation of the algorithm
		Object anchor = getAnchor(obj);

		synchronized (anchor) {
			Lockers lockers = prepareLockers(obj);

			if (hasXLock(lockers, txNum)) {
				lockers.requestQueue.remove(txNum);
				return;
			}

			try {
				String name = Thread.currentThread().getName();
				
				// long timestamp = System.currentTimeMillis();
				Long head = lockers.requestQueue.peek();
				while ((!xLockable(lockers, txNum) || (head != null && head.longValue() != txNum))
				/* && !waitingTooLong(timestamp) */) {
					
					long target = lockers.xLocker;
					if (target == -1 && !lockers.sLockers.isEmpty())
						target = lockers.sLockers.get(0);
					if (target == -1)
						target = head;
					Thread.currentThread().setName(name + " waits for xlock of " + obj + " from tx." + target);
					
					anchor.wait();
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				Thread.currentThread().setName(name);
				
				// if (!xLockable(lockers, txNum))
				// throw new LockAbortException();
				// get the x lock
				lockers.requestQueue.poll();
				lockers.xLocker = txNum;

				// An X lock blocks all other lockers, so it don't need to
				// wake up anyone.
			} catch (InterruptedException e) {
				throw new LockAbortException("Interrupted when waitting for lock");
			}
		}
		System.out.println("Tx." + txNum + " get xlock of " + obj);
	}

	/**
	 * Releases the specified type of lock on an item holding by a transaction.
	 * If a lock is the last lock on that block, then the waiting transactions
	 * are notified.
	 * 
	 * @param obj
	 *            a locked object
	 * @param txNum
	 *            a transaction number
	 * @param lockType
	 *            the type of lock
	 */
	public void release(Object obj, long txNum, LockType lockType) {
		Object anchor = getAnchor(obj);
		synchronized (anchor) {
			Lockers lks = lockerMap.get(obj);
			
			if (lks == null)
				return;
			
			releaseLock(lks, txNum, lockType, anchor);

			// Remove the locker, if there is no other transaction
			// holding it
			if (!sLocked(lks) && !xLocked(lks)
					&& lks.requestQueue.isEmpty())
				lockerMap.remove(obj);
			
			// There might be someone waiting for the lock
			anchor.notifyAll();
		}
	}

	/**
	 * Gets the anchor for the specified object.
	 * 
	 * @param obj
	 *            the target object
	 * @return the anchor for obj
	 */
	private Object getAnchor(Object obj) {
		int code = obj.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchors[code % anchors.length];
	}

	private Lockers prepareLockers(Object obj) {
		Lockers lockers = lockerMap.get(obj);
		if (lockers == null) {
			lockers = new Lockers();
			lockerMap.put(obj, lockers);
		}
		return lockers;
	}

	private void releaseLock(Lockers lks, long txNum, LockType lockType,
			Object anchor) {
		switch (lockType) {
		case X_LOCK:
			if (lks.xLocker == txNum) {
				lks.xLocker = -1;
				anchor.notifyAll();
			}
			return;
		case S_LOCK:
			List<Long> sl = lks.sLockers;
			if (sl != null && sl.contains(txNum)) {
				sl.remove((Long) txNum);
				if (sl.isEmpty())
					anchor.notifyAll();
			}
			return;
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * Verify if an item is locked.
	 */

	private boolean sLocked(Lockers lks) {
		return lks != null && lks.sLockers.size() > 0;
	}

	private boolean xLocked(Lockers lks) {
		return lks != null && lks.xLocker != -1;
	}

	/*
	 * Verify if an item is held by a tx.
	 */

	private boolean hasSLock(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.contains(txNum);
	}

	private boolean hasXLock(Lockers lks, long txNUm) {
		return lks != null && lks.xLocker == txNUm;
	}

	private boolean isTheOnlySLocker(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.size() == 1
				&& lks.sLockers.contains(txNum);
	}

	/*
	 * Verify if an item is lockable to a tx.
	 */

	private boolean sLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean xLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}
}
