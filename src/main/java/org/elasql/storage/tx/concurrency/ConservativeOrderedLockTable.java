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
package org.elasql.storage.tx.concurrency;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class ConservativeOrderedLockTable {
	enum LockType {
		IS_LOCK, IX_LOCK, S_LOCK, SIX_LOCK, X_LOCK, WRITE_BACK_LOCK
	}

	class Lockers {
		List<Long> sLockers, ixLockers, isLockers;
		// only one tx can hold xLock(sixLock) on single item
		long sixLocker, xLocker, wbLocker;
		static final long NONE = -1; // for sixLocker, xLocker and wbLocker

		Queue<Long> requestQueue, writeBackQueue;

		Lockers() {
			sLockers = new LinkedList<Long>();
			ixLockers = new LinkedList<Long>();
			isLockers = new LinkedList<Long>();
			sixLocker = NONE;
			xLocker = NONE;
			requestQueue = new LinkedList<Long>();

			wbLocker = NONE;
			writeBackQueue = new LinkedList<Long>();
		}
	}

	private Map<Object, Lockers> lockerMap = new ConcurrentHashMap<Object, Lockers>();
	private Map<Long, Set<Object>> txLockMap = new ConcurrentHashMap<Long, Set<Object>>();

	// Anchors
	private static final int NUM_ANCHOR = 1009;
	private final Object anchors[] = new Object[NUM_ANCHOR]; // To distribute
																// synchronized
																// area

	/**
	 * Create and initialize a conservative ordered lock table.
	 */
	public ConservativeOrderedLockTable() {
		// Initialize anchors
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	// TODO: Check if we needs this method
	public void requestWriteBackLocks(RecordKey[] keys, long txNum) {
		if (keys != null)
			for (RecordKey wt : keys) {
				synchronized (getAnchor(wt)) {
					Lockers lockers = prepareLockers(wt);
					lockers.writeBackQueue.add(txNum);
				}
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
	void requestLock(Object obj, long txNum) {
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
	void sLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already had lock
			if (hasSLock(lockers, txNum)) {
				lockers.requestQueue.remove(txNum);
				return;
			}

			try {
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.requestQueue.peek();
				while (!sLockable(lockers, txNum)
						|| (head != null && head.longValue() != txNum)) {

					getAnchor(obj).wait();
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				// get the s lock
				lockers.requestQueue.poll();
				lockers.sLockers.add(txNum);
				// System.out.println("S: " + obj + ", txn: " + txNum);
				getObjectSet(txNum).add(obj);

				/*
				 * Because we can let multiple transaction get s lock on the
				 * same object, it shall wake up transaction which waiting on
				 * this object and let them try to get s lock.
				 */
				getAnchor(obj).notifyAll();
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new LockAbortException(
						"Interrupted when waitting for lock");
			}
		}
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
	void xLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already had lock
			if (hasXLock(lockers, txNum)) {
				lockers.requestQueue.remove(txNum);
				return;
			}

			try {
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.requestQueue.peek();
				while (!xLockable(lockers, txNum)
						|| (head != null && head.longValue() != txNum)) {
					getAnchor(obj).wait();
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				// get the x lock
				lockers.requestQueue.poll();
				lockers.xLocker = txNum;
				// System.out.println("X: " + obj + ", txn: " + txNum);
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException(
						"Interrupted when waitting for lock");
			}
		}
	}

	/**
	 * Grants an sixlock on the specified item. If any conflict lock exists when
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
	void sixLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already had lock TODO: Add remove request
			if (hasSixLock(lockers, txNum))
				return;

			try {
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.requestQueue.peek();
				while (!sixLockable(lockers, txNum)
						|| (head != null && head.longValue() != txNum)) {
					getAnchor(obj).wait();
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				// get the six lock
				lockers.requestQueue.poll();
				lockers.sixLocker = txNum;
				getObjectSet(txNum).add(obj);
				getAnchor(obj).notifyAll();
			} catch (InterruptedException e) {
				throw new LockAbortException(
						"Interrupted when waitting for lock");
			}
		}
	}

	/**
	 * Grants an islock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * TODO: check this method
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 */
	void isLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already had lock TODO: Add remove request
			if (hasIsLock(lockers, txNum))
				return;

			try {
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.requestQueue.peek();
				while (!isLockable(lockers, txNum)
						|| (head != null && head.longValue() != txNum)) {
					getAnchor(obj).wait();
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				// get the is lock
				lockers.requestQueue.poll();
				lockers.isLockers.add(txNum);
				getObjectSet(txNum).add(obj);
				getAnchor(obj).notifyAll();
			} catch (InterruptedException e) {
				throw new LockAbortException(
						"Interrupted when waitting for lock");
			}
		}
	}

	/**
	 * Grants an ixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * TODO: check this method
	 * 
	 * @param obj
	 *            an object to be locked
	 * @param txNum
	 *            a transaction number
	 */
	void ixLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already had lock TODO: Add remove request
			if (hasIxLock(lockers, txNum))
				return;

			try {
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.requestQueue.peek();
				while (!ixLockable(lockers, txNum)
						|| (head != null && head.longValue() != txNum)) {
					getAnchor(obj).wait();
					lockers = prepareLockers(obj);
					head = lockers.requestQueue.peek();
				}

				// get the ix lock
				lockers.requestQueue.poll();
				lockers.ixLockers.add(txNum);
				getObjectSet(txNum).add(obj);
				getAnchor(obj).notifyAll();
			} catch (InterruptedException e) {
				throw new LockAbortException(
						"Interrupted when waitting for lock");
			}
		}
	}

	// TODO: check this method
	void wbLock(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			Lockers lockers = prepareLockers(obj);

			// check if it have already had lock
			if (hasWbLock(lockers, txNum)) {
				lockers.writeBackQueue.remove(txNum);
				return;
			}

			try {
				/*
				 * If this transaction is not the first one requesting this
				 * object or it cannot get lock on this object, it must wait.
				 */
				Long head = lockers.writeBackQueue.peek();
				while (!wbLockable(lockers, txNum)
						|| (head != null && head.longValue() != txNum)) {
					getAnchor(obj).wait();
					lockers = prepareLockers(obj);
					head = lockers.writeBackQueue.peek();
				}

				// get the write back lock
				lockers.writeBackQueue.poll();
				lockers.wbLocker = txNum;

				// System.out.println("WB lock" + obj + "txnum: " + txNum);
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException(
						"Interrupted when waitting for lock");
			}
		}
	}

	/**
	 * Releases the specified type of lock on an item holding by a transaction.
	 * If a lock is the last lock on that block, then the waiting transactions
	 * are notified.
	 * 
	 * TODO: Check if the lockers related to the transaction are all released.
	 * If it is true, it must remove that transaction from txLockMap. (We don't
	 * have to do this for now because all transaction will call releaseAll
	 * during committing.)
	 * 
	 * @param obj
	 *            a locked object
	 * @param txNum
	 *            a transaction number
	 * @param lockType
	 *            the type of lock
	 */
	void release(Object obj, long txNum, LockType lockType) {
		Object anchor = getAnchor(obj);
		synchronized (anchor) {
			Lockers lks = lockerMap.get(obj);
			releaseLock(lks, txNum, lockType, anchor);

			// Check if this transaction have any other lock on this object
			if (!hasSLock(lks, txNum) && !hasXLock(lks, txNum)
					&& !hasSixLock(lks, txNum) && !hasIsLock(lks, txNum)
					&& !hasIxLock(lks, txNum) && !wbLocked(lks)) {
				getObjectSet(txNum).remove(obj);

				// Remove the locker, if there is no other transaction
				// having it
				if (!sLocked(lks) && !xLocked(lks) && !sixLocked(lks)
						&& !isLocked(lks) && !ixLocked(lks) && !wbLocked(lks)
						&& lks.requestQueue.isEmpty()
						&& lks.writeBackQueue.isEmpty())
					lockerMap.remove(obj);
			}
		}
	}

	/**
	 * Releases all locks held by a transaction. If a lock is the last lock on
	 * that block, then the waiting transactions are notified.
	 * 
	 * @param txNum
	 *            a transaction number
	 */
	void releaseAll(long txNum) {

		Set<Object> objectsToRelease = getObjectSet(txNum);
		for (Object obj : objectsToRelease) {

			Object anchor = getAnchor(obj);
			synchronized (anchor) {
				Lockers lks = lockerMap.get(obj);

				if (hasSLock(lks, txNum))
					releaseLock(lks, txNum, LockType.S_LOCK, anchor);

				if (hasXLock(lks, txNum))
					releaseLock(lks, txNum, LockType.X_LOCK, anchor);

				if (hasSixLock(lks, txNum))
					releaseLock(lks, txNum, LockType.SIX_LOCK, anchor);

				if (hasIsLock(lks, txNum))
					releaseLock(lks, txNum, LockType.IS_LOCK, anchor);

				if (hasIxLock(lks, txNum))
					releaseLock(lks, txNum, LockType.IX_LOCK, anchor);

				if (hasWbLock(lks, txNum))
					releaseLock(lks, txNum, LockType.WRITE_BACK_LOCK, anchor);

				// Remove the locker, if there is no other transaction
				// having it
				// if (lks == null) {
				// System.out.println("!!! Release: " + obj + "txnum: "
				// + txNum);
				// } else {
				// System.out.println("Release: " + obj + "txnum: " + txNum);
				// }
				if (!sLocked(lks) && !xLocked(lks) && !sixLocked(lks)
						&& !isLocked(lks) && !ixLocked(lks) && !wbLocked(lks)
						&& lks.requestQueue.isEmpty()
						&& lks.writeBackQueue.isEmpty())
					lockerMap.remove(obj);
			}
		}
		// Remove the locked object set of the corresponding transaction
		txLockMap.remove(txNum);
		/*
		 * TODO: Sometimes there are some object which will be requested but
		 * that requesting transaction do not actually get the lock. We may have
		 * to check if it happened.
		 */

	}

	/**
	 * To decide which anchor object obj should use.
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

	private Set<Object> getObjectSet(long txNum) {
		Set<Object> objectSet = txLockMap.get(txNum);
		if (objectSet == null) {
			objectSet = new HashSet<Object>();
			txLockMap.put(txNum, objectSet);
		}
		return objectSet;
	}

	private void releaseLock(Lockers lks, long txNum, LockType lockType,
			Object anchor) {
		if (lks == null)
			return;

		switch (lockType) {
		case X_LOCK:
			if (lks.xLocker == txNum) {
				lks.xLocker = -1;
				anchor.notifyAll();
			}
			return;
		case SIX_LOCK:
			if (lks.sixLocker == txNum) {
				lks.sixLocker = -1;
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
		case IS_LOCK:
			List<Long> isl = lks.isLockers;
			if (isl != null && isl.contains(txNum)) {
				isl.remove((Long) txNum);
				if (isl.isEmpty())
					anchor.notifyAll();
			}
			return;
		case IX_LOCK:
			List<Long> ixl = lks.ixLockers;
			if (ixl != null && ixl.contains(txNum)) {
				ixl.remove((Long) txNum);
				if (ixl.isEmpty())
					anchor.notifyAll();
			}
			return;
		case WRITE_BACK_LOCK:
			if (lks.wbLocker == txNum) {
				lks.wbLocker = -1;
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

	private boolean wbLocked(Lockers lks) {
		return lks != null && lks.wbLocker != -1;
	}

	private boolean sLocked(Lockers lks) {
		return lks != null && lks.sLockers.size() > 0;
	}

	private boolean xLocked(Lockers lks) {
		return lks != null && lks.xLocker != -1;
	}

	private boolean sixLocked(Lockers lks) {
		return lks != null && lks.sixLocker != -1;
	}

	private boolean isLocked(Lockers lks) {
		return lks != null && lks.isLockers.size() > 0;
	}

	private boolean ixLocked(Lockers lks) {
		return lks != null && lks.ixLockers.size() > 0;
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

	private boolean hasWbLock(Lockers lks, long txNUm) {
		return lks != null && lks.wbLocker == txNUm;
	}

	private boolean hasSixLock(Lockers lks, long txNum) {
		return lks != null && lks.sixLocker == txNum;
	}

	private boolean hasIsLock(Lockers lks, long txNum) {
		return lks != null && lks.isLockers.contains(txNum);
	}

	private boolean hasIxLock(Lockers lks, long txNum) {
		return lks != null && lks.ixLockers.contains(txNum);
	}

	private boolean isTheOnlySLocker(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.size() == 1
				&& lks.sLockers.contains(txNum);
	}

	private boolean isTheOnlyIsLocker(Lockers lks, long txNum) {
		if (lks != null) {
			for (Object o : lks.isLockers)
				if (!o.equals(txNum))
					return false;
			return true;
		}
		return false;
	}

	private boolean isTheOnlyIxLocker(Lockers lks, long txNum) {
		if (lks != null) {
			for (Object o : lks.ixLockers)
				if (!o.equals(txNum))
					return false;
			return true;
		}
		return false;
	}

	/*
	 * Verify if an item is lockable to a tx.
	 */

	private boolean wbLockable(Lockers lks, long txNum) {
		return (!wbLocked(lks) || hasWbLock(lks, txNum));
	}

	private boolean sLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum));
	}

	private boolean xLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum))
				&& (!isLocked(lks) || isTheOnlyIsLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean sixLockable(Lockers lks, long txNum) {
		return (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum))
				&& (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean ixLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean isLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum));
	}
}
