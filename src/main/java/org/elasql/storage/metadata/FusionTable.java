package org.elasql.storage.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.util.PeriodicalJob;

public class FusionTable {
	
	private static final int DEFAULT_SIZE = 100_000;
	
	class LocationRecord {
		RecordKey key; // null => not used
		int partId;
		int nextFreeSlotId; // free chain (increase the speed to search free space)
		boolean referenced; // for clock replacement strategy
	}
	
	private int expMaxSize;
	private int size;
	private int firstFreeSlot;
	private LocationRecord[] locations;
	private Map<RecordKey, Integer> keyToSlotIds;
	private Map<RecordKey, Integer> overflowedKeys;
	private int nextSlotToReplace;
	
	// Tracking
	private int[] countsPerParts = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private static class HitCounter {
		int total = 0;
		int hit = 0;
		
		synchronized void hit() {
			total++;
			hit++;
		}
		
		synchronized void miss() {
			total++;
		}
		
		synchronized HitCounter output() {
			HitCounter cloned = new HitCounter();
			cloned.total = total;
			cloned.hit = hit;
			total = 0;
			hit = 0;
			return cloned;
		}
	}
	private HitCounter hitCounter = new HitCounter();
	
	/**
	 * Create a fusion table with the expected maximum capacity.
	 * Note that the size of table may exceed the given size.
	 * The exceeded space is freed only when someone calls removedOverflowKeys.
	 * 
	 * @param expectedMaxSize
	 */
	public FusionTable(int expectedMaxSize) {
		if (expectedMaxSize == -1)
			expMaxSize = DEFAULT_SIZE;
		else
			expMaxSize = expectedMaxSize;
		size = 0;
		firstFreeSlot = 0;
		locations = new LocationRecord[expMaxSize];
		for (int i = 0; i < expMaxSize; i++) {
			locations[i] = new LocationRecord();
			if (i != expMaxSize - 1)
				locations[i].nextFreeSlotId = i + 1;
			else
				locations[i].nextFreeSlotId = -1;
		}
		keyToSlotIds = new HashMap<RecordKey, Integer>(expMaxSize);
//		keyToSlotIds = new ConcurrentHashMap<RecordKey, Integer>(expMaxSize);
		overflowedKeys = new HashMap<RecordKey, Integer>();
		nextSlotToReplace = 0;
		
//		new PeriodicalJob(10_000, 1200_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				System.out.println(String.format("Time: %d seconds, Total Size: %d, Overflow Size: %d",
//						time, size, overflowedKeys.size()));
//			}
//		}).start();
		
//		new PeriodicalJob(5_000, 2400_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				
//				StringBuffer sb = new StringBuffer();
//				sb.append(String.format("Time: %d seconds - ", time));
//				for (int i = 0; i < countsPerParts.length; i++)
//					sb.append(String.format("%d, ", countsPerParts[i]));
//				sb.delete(sb.length() - 2, sb.length());
//				
//				System.out.println(sb.toString());
//			}
//		}).start();
		
//		new PeriodicalJob(5_000, 2400_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				
//				// Initial table names
//				String[] tableNames = new String[] {"warehouse", "district", "stock",
//							"customer", "history", "orders", "new_order", "order_line",
//				            "item"};
//				Map<String, Integer> tableToIdx = new HashMap<String, Integer>();
//				for (int i = 0; i < tableNames.length; i++)
//					tableToIdx.put(tableNames[i], i);
//				
//				// Calculate table names
//				Set<RecordKey> keys = new HashSet<RecordKey>(keyToSlotIds.keySet());
//				int[] counts = new int[tableNames.length];
//				for (RecordKey key : keys) {
//					int idx = tableToIdx.get(key.getTableName());
//					counts[idx]++;
//				}
//				
//				// Output the result
//				StringBuffer sb = new StringBuffer();
//				sb.append(String.format("Time: %d seconds - ", time));
//				for (int i = 0; i < tableNames.length; i++)
//					sb.append(String.format("%d, ", counts[i]));
//				sb.delete(sb.length() - 2, sb.length());
//				
//				System.out.println(sb.toString());
//			}
//		}).start();
		
//		new PeriodicalJob(10_000, 1200_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				
//				HitCounter result = hitCounter.output();
//				double hitRate = 0.0;
//				if (result.total > 0) {
//					hitRate = ((double) result.hit) / result.total * 100;
//				}
//				System.out.println(String.format("Time: %d seconds, Hit Rate: %.2f%% (hits: %d, total: %d)",
//						time, hitRate, result.hit, result.total));
//			}
//		}).start();
	}
	
	public void setLocation(RecordKey key, int partId) {
		Integer slotId = keyToSlotIds.get(key);
		
		if (slotId != null) {
			countsPerParts[locations[slotId].partId]--;
			locations[slotId].partId = partId;
			locations[slotId].referenced = true;
		} else {
			if (overflowedKeys.containsKey(key)) {
				countsPerParts[overflowedKeys.get(key)]--;
				overflowedKeys.put(key, partId);
			} else
				insertNewRecord(key, partId);
		}
		
		countsPerParts[partId]++;
	}
	
	public int getLocation(RecordKey key) {
		Integer slotId = keyToSlotIds.get(key);
		
		if (slotId != null) {
//			hitCounter.hit();
			locations[slotId].referenced = true;
			return locations[slotId].partId;
		} else {
			Integer partId = overflowedKeys.get(key);
			if (partId != null) {
//				hitCounter.hit();
				return partId;
			} else {
//				hitCounter.miss();
				return -1;
			}
		}
	}
	
	public boolean containsKey(RecordKey key) {
		return keyToSlotIds.containsKey(key) || overflowedKeys.containsKey(key);
	}
	
	/**
	 * Remove the record of the location of the given key.
	 * 
	 * @param key
	 * @return the partition id in the record
	 */
	public int remove(RecordKey key) {
		Integer slotId = keyToSlotIds.remove(key);
		
		if (slotId != null) {
			locations[slotId].key = null;
			size--;
			countsPerParts[locations[slotId].partId]--;
			
			// add to the free chain
			locations[slotId].nextFreeSlotId = firstFreeSlot;
			firstFreeSlot = slotId;
			
			return locations[slotId].partId;
		} else {
			Integer partId = overflowedKeys.remove(key);
			if (partId != null) {
				size--;
				countsPerParts[partId]--;
				return partId;
			} else
				return -1;
		}
	}
	
	public int size() {
		return size;
	}
	
	@Deprecated
	public Map<RecordKey, Integer> removeOverflowKeys() {
		Map<RecordKey, Integer> removedKeys = overflowedKeys;
		overflowedKeys = new HashMap<RecordKey, Integer>();
		size -= removedKeys.size();
		return removedKeys;
	}
	
	public Set<RecordKey> getOverflowKeys() {
		return new HashSet<RecordKey>(overflowedKeys.keySet());
	}
	
	private void insertNewRecord(RecordKey key, int partId) {
		int freeSlot = findFreeSlot();
		if (freeSlot == -1)
			freeSlot = swapOutRecord();
		
		locations[freeSlot].key = key;
		locations[freeSlot].partId = partId;
		locations[freeSlot].referenced = true;
		keyToSlotIds.put(key, freeSlot);
		size++;
	}
	
	private int findFreeSlot() {
		if (firstFreeSlot != -1) {
			int freeSlot = firstFreeSlot;
			firstFreeSlot = locations[freeSlot].nextFreeSlotId;
			return freeSlot;
		} else { // no free slot
			return -1;
		}
	}
	
	private int swapOutRecord() {
		// Select a slot (using clock)
		while (locations[nextSlotToReplace].referenced) {
			locations[nextSlotToReplace].referenced = false;
			nextSlotToReplace = (nextSlotToReplace + 1) % expMaxSize;
		}
		int swapSlot = nextSlotToReplace;
		nextSlotToReplace = (nextSlotToReplace + 1) % expMaxSize;
		
		// Swap out the content of the slot
		keyToSlotIds.remove(locations[swapSlot].key);
		overflowedKeys.put(locations[swapSlot].key, locations[swapSlot].partId);
		locations[swapSlot].key = null;
		
		return swapSlot;
	}
}
