package org.elasql.storage.tx.concurrency;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.sql.PrimaryKey;

/**
 * FifoOrderedLockTable is actually a ConservativeOrderedLockTable. However, in
 * FifoOrderedLockTable implementation, notifyAll, which might be a performance
 * killer, is no longer exist.
 * 
 * @author Pin-Yu Wang, Yu-Shan Lin
 *
 */
public class FifoOrderedLockTable {
	/**
	 * lockerMap maps Objects to FifoLockers. Be aware that the key type is declared
	 * as Object though, it is type of PrimaryKey under the hood.
	 * 
	 * TODO: In the current implementation, key value entries won't be removed. This
	 * might cause memory leak if the key value entries keeps growing.
	 */
	private ConcurrentHashMap<Object, FifoLockers> lockerMap = new ConcurrentHashMap<Object, FifoLockers>();

	/**
	 * RequestLock will add the given fifoLock into the requestQueue of the given
	 * primaryKey. Object is type of PrimaryKey under the hood.
	 * 
	 * @param obj
	 * @param fifoLock
	 */
	void requestLock(Object obj, FifoLock fifoLock) {
		/*
		 * putIfAbsent is an atomic operation. It's OK to let two threads put a new
		 * FifoLockers into the lockerMap because the second thread's fifoLockers will
		 * be ignored. After that, the two threads will get the exactly same FifoLockers
		 * from the map again.
		 */
	    lockerMap.putIfAbsent(obj, new FifoLockers());
		FifoLockers lks = lockerMap.get(obj);

		lks.addToRequestQueue(fifoLock);
	}

	void sLock(Object obj, FifoLock fifoLock) {
		FifoLockers lks = lockerMap.get(obj);

		lks.waitOrPossessSLock(fifoLock);
	}

	void xLock(Object obj, FifoLock fifoLock) {
		FifoLockers lks = lockerMap.get(obj);

		lks.waitOrPossessXLock(fifoLock);
	}

	void releaseSLock(Object obj, FifoLock myFifoLock) {
		FifoLockers lks = lockerMap.get(obj);

		lks.releaseSLock(myFifoLock);
	}

	void releaseXLock(Object obj, FifoLock myFifoLock) {
		FifoLockers lks = lockerMap.get(obj);

		lks.releaseXLock(myFifoLock);
	}
}
