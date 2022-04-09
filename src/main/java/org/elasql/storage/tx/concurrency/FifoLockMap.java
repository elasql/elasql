package org.elasql.storage.tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import org.elasql.sql.PrimaryKey;

/**
 * For every transaction, there will be a FifoLockMap in the transaction life
 * cycle. A FifoLockMap maps PrimaryKeys to FifoLocks. FiflLockMap is need
 * because transactions will wait on there "OWN" fifoLock instead of a shared
 * object if they need the same record. The shared object is called "anchor" in
 * the legacy code. Waiting on a shared object will involve triggering notifyALL
 * due to the limitation of java, which might be a performance killer.
 * 
 * @author Pin-Yu Wang
 *
 */
public class FifoLockMap {
	private Map<PrimaryKey, FifoLock> map = new HashMap<PrimaryKey, FifoLock>();
	
	/**
	 * This method will put a new FifoLock as value in the map and the key is PrimaryKey.
	 */
	public FifoLock registerLock(PrimaryKey key) {
		return map.putIfAbsent(key, new FifoLock());
	}
}
