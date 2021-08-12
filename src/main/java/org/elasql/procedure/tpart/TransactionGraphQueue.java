package org.elasql.procedure.tpart;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.sql.PrimaryKey;

public class TransactionGraphQueue {
	class LockInfo {
		long txNum;
		Boolean isReadOnly;

		LockInfo(long txNum, Boolean isReadOnly) {
			this.txNum = txNum;
			this.isReadOnly = isReadOnly;
		}
	}

    private static final int NUM_ANCHOR = 1009;
    
	private Map<Object, LinkedList<LockInfo>> rwLockQueueMap = new ConcurrentHashMap<Object, LinkedList<LockInfo>>();
    // Lock-stripping
	private final Object anchors[] = new Object[NUM_ANCHOR];

    public TransactionGraphQueue(){
        // Initialize anchors
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
    }

    /**
	 * Gets the anchor for the specified object.
	 * 
	 * @param obj the target object
	 * @return the anchor for obj
	 */
	private Object getAnchor(Object obj) {
		int code = obj.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchors[code % anchors.length];
	}

    // MODIFIED:
	public void addSLockRequest(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			LinkedList<LockInfo> queue = prepareQueue(obj);
			LockInfo info = new LockInfo(txNum, true);
			// if (queue != null) {
			queue.add(info);
			// }
		}
	}

	// MODIFIED:
	public void addXLockRequest(Object obj, long txNum) {
		synchronized (getAnchor(obj)) {
			LinkedList<LockInfo> queue = prepareQueue(obj);
			LockInfo info = new LockInfo(txNum, false);
			// if (queue != null) {
			queue.clear();
			queue.add(info);
			// }
		}
	}

	// MODIFIED:
	public void addSLockRequests(Collection<PrimaryKey> keys, long txNum) {
		if(keys != null){
			for(PrimaryKey key : keys){
				if(key != null)
					addSLockRequest(key, txNum);
			}
		}
	}

	// MODIFIED:
	public void addXLockRequests(Collection<PrimaryKey> keys, long txNum) {
		if(keys != null){
			for(PrimaryKey key : keys){
				if(key != null)
					addXLockRequest(key, txNum);
			}
		}
	}

	// MODIFIED:
	LinkedList<LockInfo> prepareQueue(Object obj) {
		LinkedList<LockInfo> queue = rwLockQueueMap.get(obj);
		if (queue == null) {
			queue = new LinkedList<LockInfo>();
			rwLockQueueMap.put(obj, queue);
		}
		return queue;
	}

	// MODIFIED:
	public Set<Long> checkPreviousWaitingTxns(Object obj, Boolean isReadOnly) {
		Set<Long> dependentTxns = new HashSet<Long>();

		synchronized (getAnchor(obj)) {
			LinkedList<LockInfo> queue = prepareQueue(obj);

			if (queue.size() > 0) {
				Iterator<LockInfo> iter = queue.descendingIterator();
				while (iter.hasNext()) {
					LockInfo info = iter.next();

					if (isReadOnly) {
						// If Read-Only
						if (info.isReadOnly == false) {
							dependentTxns.add(info.txNum);
							break;
						}
					} else {
						// If R/W
						if (info.isReadOnly == true) {
							dependentTxns.add(info.txNum);
						} else {
							if(dependentTxns.size() <= 0){}
								dependentTxns.add(info.txNum);
							break;
						}
					}
				}
			}
		}
		// If queue is empty
		return dependentTxns;
	}
}
