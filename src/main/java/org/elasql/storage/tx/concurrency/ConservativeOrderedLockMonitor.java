package org.elasql.storage.tx.concurrency;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConservativeOrderedLockMonitor {
	
	private static final int MOVING_WINDOW = 1000;
	
	//xLock statistics
	private static Queue<Long> xLockLatencies  = new ConcurrentLinkedQueue<Long>();
	private static AtomicLong xLockLatencySMA = new AtomicLong();
	private static AtomicLong xLockLatencyWMA = new AtomicLong();
	
	public ConservativeOrderedLockMonitor() {
		//Do nothing
	}
	
	public static void AddxLockWaitTime(long waitTime) {
		if (xLockLatencies == null)
			return;
		
		xLockLatencies.add(waitTime);

		if(xLockLatencies.size() >= MOVING_WINDOW) {
			long popped = xLockLatencies.poll();
			
			calculateSMA(popped, waitTime);
			//calculateWMA(waitTime);
			
			return;
		}
		
		calculateSMA(0, waitTime);
		//calculateWMA(waitTime);
		
		return;
	}
	
	public static Queue<Long> getxLockWaitTime() {
		return xLockLatencies;
	}
	
	public static long getxLockWaitTimeSMA() {
		return xLockLatencySMA.get();
	}
	
	public static long getxLockWaitTimeWMA() {
		return xLockLatencyWMA.get();
	}
	
	private static void calculateSMA(long popped, long waitTime) {
		long temp_sum = xLockLatencySMA.get() * xLockLatencies.size() - popped;
		temp_sum += waitTime;
		if(xLockLatencies.size() > 0)
			xLockLatencySMA.set(temp_sum/ xLockLatencies.size());
		else
			xLockLatencySMA.set(temp_sum);
		return;
	}
	
	private static void calculateWMA(long waitTime) {
		long temp_sum = 0;
		int weight = 1;
		float weight_decay = 0.1f;
		
		if(xLockLatencies.size() > 0) {
			for(long item : xLockLatencies) {
				temp_sum += item * weight;
				weight += weight_decay;
			}
		}
		
		temp_sum += waitTime * weight;
		
		float total_denominator = (1 + weight) * xLockLatencies.size() / 2;
		if(total_denominator > 0)
			xLockLatencyWMA.set((long)(temp_sum/total_denominator));
		else
			xLockLatencyWMA.set(temp_sum);
		return;
	}
}
