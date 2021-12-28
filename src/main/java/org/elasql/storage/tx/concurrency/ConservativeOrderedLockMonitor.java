package org.elasql.storage.tx.concurrency;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConservativeOrderedLockMonitor {
	
	// Window sizes go beyond this amount are pretty much meaningless.
	// Since it will be dominated by huge peaks.
	private static final int MOVING_WINDOW = 10;
	// This number should be the same as ConservativeOrderedLockTable's number. 
	private static final int NUM_ANCHOR = 1009;
	
	// xLock statistics
	private static Queue<Long> xLockLatencies  = new ConcurrentLinkedQueue<Long>();
	private static AtomicLong xLockLatencySimpleMovingAverage = new AtomicLong();
	private static AtomicLong xLockLatencyWeightedMovingAverage = new AtomicLong();
	private static int sLock_anchor_counter[] = new int[NUM_ANCHOR];
	private static int sLock_anchor_conflict_counter[] = new int[NUM_ANCHOR];
	private static int xLock_anchor_counter[] = new int[NUM_ANCHOR];
	private static int xLock_anchor_conflict_counter[] = new int[NUM_ANCHOR];
	
	
	public ConservativeOrderedLockMonitor() {
		// Do nothing
		for (int i = 0; i < sLock_anchor_counter.length; i++) {
			sLock_anchor_counter[i] = 0;
			sLock_anchor_conflict_counter[i] = 0;
			xLock_anchor_counter[i] = 0;
			xLock_anchor_conflict_counter[i] = 0;
		}
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
		return;
	}
	
	public static Queue<Long> getxLockWaitTime() {
		return xLockLatencies;
	}
	
	public static long getxLockWaitTimeSMA() {
		System.out.println("getxLockWaitTimeSMA reutrn: " + xLockLatencySimpleMovingAverage.get());
		return xLockLatencySimpleMovingAverage.get();
	}
	
	public static long getxLockWaitTimeWMA() {
		return xLockLatencyWeightedMovingAverage.get();
	}
	
	// TODO: Make sure synchronized
	private static void calculateSimpleMovingAverage(long popped, long waitTime) {
		
		long temp_sum = xLockLatencySimpleMovingAverage.get() * xLockLatencies.size() - popped;
		temp_sum += waitTime;
		
		// Prevent it somehow become negative.
		if(temp_sum < 0)
			temp_sum = 0;
		
		if(xLockLatencies.size() > 0)
			xLockLatencySimpleMovingAverage.set(temp_sum/ xLockLatencies.size());
		else
			xLockLatencySimpleMovingAverage.set(temp_sum);
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
	
	public static void raiseAnchorCounter(int anchor, String lock, boolean conflict) {
		if(lock == "sLock") {
			sLock_anchor_counter[anchor] += 1;
			if(conflict)
				sLock_anchor_conflict_counter[anchor] += 1;
		}
		else {
			xLock_anchor_counter[anchor] += 1;
			if(conflict)
				xLock_anchor_conflict_counter[anchor] += 1;
		}
		//System.out.println("sLock Counter: " + sLock_anchor_counter[LOG_TARGET]);
		//Arrays.stream(sLock_anchor_counter).forEach(System.out::println);
		//System.out.println("xLock Counter: " + xLock_anchor_counter[LOG_TARGET+1]);
		
	}
	
	public static int[] getxLockAnchorCounter() {
		return xLock_anchor_counter;
	}
	
	public static int[] getsLockAnchorCounter() {
		return sLock_anchor_counter;
	}
	
	public static int[] getxLockAnchorConflictCounter() {
		return xLock_anchor_conflict_counter;
	}
	
	public static int[] getsLockAnchorConflictCounter() {
		return sLock_anchor_conflict_counter;
	}
}
