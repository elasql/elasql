package org.elasql.schedule.tpart.hermes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class FusionTable {
	
	public static final int EXPECTED_MAX_SIZE;
	
	static {
		EXPECTED_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(FusionTable.class.getName() + ".EXPECTED_MAX_SIZE", 100_000);
	}
	
	class LocationRecord {
		PrimaryKey key; // null => not used
		int partId;
		int nextFreeSlotId; // free chain (increase the speed to search free space)
		boolean referenced; // for clock replacement strategy
	}
	
	private int size;
	private int firstFreeSlot;
	private LocationRecord[] locations;
	private Map<PrimaryKey, Integer> keyToSlotIds;
	private Map<PrimaryKey, Integer> overflowedKeys;
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
	 * Create a fusion table
	 */
	public FusionTable() {
		size = 0;
		firstFreeSlot = 0;
		locations = new LocationRecord[EXPECTED_MAX_SIZE];
		for (int i = 0; i < EXPECTED_MAX_SIZE; i++) {
			locations[i] = new LocationRecord();
			if (i != EXPECTED_MAX_SIZE - 1)
				locations[i].nextFreeSlotId = i + 1;
			else
				locations[i].nextFreeSlotId = -1;
		}
		keyToSlotIds = new HashMap<PrimaryKey, Integer>(EXPECTED_MAX_SIZE);
//		keyToSlotIds = new ConcurrentHashMap<RecordKey, Integer>(expMaxSize);
		overflowedKeys = new HashMap<PrimaryKey, Integer>();
		nextSlotToReplace = 0;
		
		// Debug: Show the statistics of the fusion table
//		new PeriodicalJob(10_000, 1200_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				System.out.println(String.format("Time: %d seconds, Total Size: %d, Overflow Size: %d",
//						time, size, overflowedKeys.size()));
//			}
//		}).start();
		
		// Debug: Show how many records are cached in each partition
//		new PeriodicalJob(3_000, 360_000, new Runnable() {
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
		
		// Debug: show how many records are cached in each table
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
		
		// Debug: show hit rate
//		new PeriodicalJob(5_000, 360_000, new Runnable() {
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
	
	public void setLocation(PrimaryKey key, int partId) {
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
	
	public int getLocation(PrimaryKey key) {
		Integer slotId = keyToSlotIds.get(key);
		
		if (slotId != null) {
			hitCounter.hit();
			locations[slotId].referenced = true;
			return locations[slotId].partId;
		} else {
			Integer partId = overflowedKeys.get(key);
			if (partId != null) {
				hitCounter.hit();
				return partId;
			} else {
				hitCounter.miss();
				return -1;
			}
		}
	}
	
	public boolean containsKey(PrimaryKey key) {
		return keyToSlotIds.containsKey(key) || overflowedKeys.containsKey(key);
	}
	
	/**
	 * Remove the record of the location of the given key.
	 * 
	 * @param key
	 * @return the partition id in the record
	 */
	public int remove(PrimaryKey key) {
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
	public Map<PrimaryKey, Integer> removeOverflowKeys() {
		Map<PrimaryKey, Integer> removedKeys = overflowedKeys;
		overflowedKeys = new HashMap<PrimaryKey, Integer>();
		size -= removedKeys.size();
		return removedKeys;
	}
	
	public Set<PrimaryKey> getOverflowKeys() {
		return new HashSet<PrimaryKey>(overflowedKeys.keySet());
	}
	
	private void insertNewRecord(PrimaryKey key, int partId) {
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
			nextSlotToReplace = (nextSlotToReplace + 1) % EXPECTED_MAX_SIZE;
		}
		int swapSlot = nextSlotToReplace;
		nextSlotToReplace = (nextSlotToReplace + 1) % EXPECTED_MAX_SIZE;
		
		// Swap out the content of the slot
		keyToSlotIds.remove(locations[swapSlot].key);
		overflowedKeys.put(locations[swapSlot].key, locations[swapSlot].partId);
		locations[swapSlot].key = null;
		
		return swapSlot;
	}
}
