package org.elasql.storage.tx.concurrency;

import java.util.concurrent.ConcurrentHashMap;

import org.elasql.storage.tx.concurrency.fifolocker.FifoLock;
import org.elasql.storage.tx.concurrency.fifolocker.FifoLockers;

public class FifoOrderedLockTable {
	/**
	 * lockerMap maps Objects to FifoLockers. Be aware that the key type is declared
	 * as Object though, it is type of PrimaryKey under the hood.
	 */
	private ConcurrentHashMap<Object, FifoLockers> lockerMap = new ConcurrentHashMap<Object, FifoLockers>();

	/**
	 * RequestLock will add the given fifoLock into the requestQueue of the given primaryKey. 
	 * Object is type of PrimaryKey under the hood.
	 * @param obj
	 * @param fifoLock
	 */
	void requestLock(Object obj, FifoLock fifoLock) {
		/*
		 * PutIfAbsent is an atomic operation. There will be no race condition on
		 * getting and setting FifoLockers.
		 */
		FifoLockers lks = lockerMap.putIfAbsent(obj, new FifoLockers());
		lks.addToRequestQueue(fifoLock);
	}

	void sLock(Object obj, long txNum) {
		FifoLockers lks = lockerMap.get(obj);
		
		lks.waitOrPossessSLock(txNum);
	}
	
	void xLock(Object obj, long txNum) {
		FifoLockers lks = lockerMap.get(obj);
		
		lks.waitOrPossessXLock(txNum);
	}
	
	void releaseSLock(Object obj, long txNum) {
		FifoLockers lks = lockerMap.get(obj);
		
		lks.releaseSLock(txNum);
	}
	
	void releaseXLock(Object obj, long txNum) {
		FifoLockers lks = lockerMap.get(obj);
		
		lks.releaseXLock(txNum);
	}
}
