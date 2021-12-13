package org.elasql.storage.tx.concurrency;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.vanilladb.core.util.TransactionProfiler;

public class ConservativeOrderedLockMonitor {
	
	private static final int MOVING_WINDOW = 1000;
	
	//xLock statistics
	private static Queue<Long> xLockLatencies;
	private static AtomicLong xLockLatency;
	
	public ConservativeOrderedLockMonitor() {
		xLockLatencies = new LinkedList<Long>();
		xLockLatency = new AtomicLong();
	}
	
	public void AddxLockWaitTime(long waitTime) {
		if(xLockLatencies.size() >= MOVING_WINDOW) {
			long popped = xLockLatencies.poll();
			long temp_sum = xLockLatency.get() * (MOVING_WINDOW) - popped;
			
			xLockLatencies.add(waitTime);
			temp_sum += waitTime;
			xLockLatency.set(temp_sum / MOVING_WINDOW);
		}
		
		long temp_sum = xLockLatency.get() * xLockLatencies.size();
		xLockLatencies.add(waitTime);
		temp_sum += waitTime;
		
		xLockLatency.set(temp_sum / xLockLatencies.size());
	}
	
	public Queue<Long> getxLockWaitTime() {
		return xLockLatencies;
	}
	
	public long getxLockWaitTimeSMA() {
		return xLockLatency.get();
	}
	
}
