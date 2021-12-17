package org.elasql.storage.tx.concurrency;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConservativeOrderedLockMonitor {
	
	//Window sizes go beyond this amount are pretty much meaningless.
	//Since it will be dominated by huge peaks.
	private static final int MOVING_WINDOW = 10;
	
	//xLock statistics
	private static Queue<Long> xLockLatencies  = new ConcurrentLinkedQueue<Long>();
	private static AtomicLong xLockLatencySimpleMovingAverage = new AtomicLong();
	private static AtomicLong xLockLatencyWeightedMovingAverage = new AtomicLong();
	private static long xLockLatencySimpleMovingAverage_cache = 0;
	
	public ConservativeOrderedLockMonitor() {
		//Do nothing
	}
	
	public static void AddxLockWaitTime(long waitTime) {
		if (xLockLatencies == null)
			return;
		
		System.out.println("WaitTime: " + waitTime);
		long popped = 0;
		synchronized(xLockLatencies) {
			xLockLatencies.add(waitTime);
			if(xLockLatencies.size() >= MOVING_WINDOW) {
				popped = xLockLatencies.poll();
				System.out.println("popped: " + popped);
				calculateSimpleMovingAverage(popped, waitTime);
			}
		}
			
		//calculateWeightedMovingAverage(waitTime);
		System.out.println("xLockLatencySimpleMovingAverage: " + xLockLatencySimpleMovingAverage);
		System.out.println("xLockLatencySimpleMovingAverage_cache: " + xLockLatencySimpleMovingAverage_cache);
		
		return;
	}
	
	public static Queue<Long> getxLockWaitTime() {
		return xLockLatencies;
	}
	
	public static long getxLockWaitTimeSMA() {
		System.out.println("getxLockWaitTimeSMA reutrn: " + xLockLatencySimpleMovingAverage_cache);
		return xLockLatencySimpleMovingAverage_cache;
	}
	
	public static long getxLockWaitTimeWMA() {
		return xLockLatencyWeightedMovingAverage.get();
	}
	
	// TODO: Make sure synchronized
	private static void calculateSimpleMovingAverage(long popped, long waitTime) {
		
		long temp_sum = xLockLatencySimpleMovingAverage.get() * xLockLatencies.size() - popped;
		temp_sum += waitTime;
		if(xLockLatencies.size() > 0)
			xLockLatencySimpleMovingAverage.set(temp_sum/ xLockLatencies.size());
		else
			xLockLatencySimpleMovingAverage.set(temp_sum);
		xLockLatencySimpleMovingAverage_cache = xLockLatencySimpleMovingAverage.longValue();
		return;
	}
	
	private static void calculateWeightedMovingAverage(long waitTime) {
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
			xLockLatencyWeightedMovingAverage.set((long)(temp_sum/total_denominator));
		else
			xLockLatencyWeightedMovingAverage.set(temp_sum);
		return;
	}
}
