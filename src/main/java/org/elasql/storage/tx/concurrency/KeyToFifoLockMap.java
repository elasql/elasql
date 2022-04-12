package org.elasql.storage.tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.concurrency.fifolocker.FifoLock;

/**
 * KeyToFifoLockMap is a transaction-local object alive in the transaction life
 * cycle. A FifoLockMap maps PrimaryKeys to FifoLocks. KeyToFifoLockMap is need
 * because transactions will wait on there "OWN" fifoLock instead of a shared
 * object if all of them need the same record. The shared object is called
 * "anchor" in the legacy code. Waiting on a shared object will involve
 * triggering notifyALL due to the limitation of java, which might be a
 * performance killer.
 * 
 * @author Pin-Yu Wang
 *
 */
public class KeyToFifoLockMap {
	private Map<PrimaryKey, FifoLock> map = new HashMap<PrimaryKey, FifoLock>();

	/**
	 * This method will put a new FifoLock as value in the map and the key is
	 * PrimaryKey.
	 */
	public FifoLock registerKey(PrimaryKey key, long txNum) {
//		if (!map.containsKey(key)) {
//			map.put(key, new FifoLock(key, txNum));
//		} else {
//			// XXX: Registering a key twice will make system stall due to the new
//			// locking implementation.
//			throw new RuntimeException("Registering a key twice is not allowed");
//		}
		
		FifoLock fifoLock = new FifoLock(key, txNum);
		map.put(key, fifoLock);
		
		return map.get(key);
	}

	public FifoLock lookForFifoLock(Object key) {
		return map.get(key);
	}
}
