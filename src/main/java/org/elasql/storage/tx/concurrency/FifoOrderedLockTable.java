package org.elasql.storage.tx.concurrency;

import java.util.concurrent.ConcurrentHashMap;

public class FifoOrderedLockTable {
	/**	
	 * Although the key type is declared as Object,
	 * it is type of PrimaryKey under the hood.
	 */
	private ConcurrentHashMap<Object, RequestQueue> lockerMap = new ConcurrentHashMap<Object, RequestQueue>();

	void requestLock(Object obj, long txNum) {
		/*
		 * PutIfAbsent is an atomic operation.
		 * There will be no race condition.
		 */
		lockerMap.putIfAbsent(obj, new RequestQueue());
		
	}
}
