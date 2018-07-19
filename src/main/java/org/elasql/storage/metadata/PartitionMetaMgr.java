/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.storage.metadata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;

public class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;
//	public final static File LOGDIR;
//	public final static File LOGFILE;
	public static FileWriter WRLOGFILE;
	public static BufferedWriter BWRLOGFILE;
//	private static final long BENCH_START_TIME;
	
	private static Map<RecordKey, Integer> locationTable;
	private static enum PickingMethods { NO, FIFO, LRU, CLOCK };
	private static final PickingMethods PICKING_METHOD = PickingMethods.FIFO;
	public static final int LOC_TABLE_MAX_SIZE;
	// TODO: Maybe we could limit the size of the queue by 2 x LOC_TABLE_MAX_SIZE
	private static Queue<RecordKey> fifoQueue = new LinkedList<RecordKey>();
	
	private PartitionPlan partPlan;
	private boolean isInMigration;

	static {

//		LOGDIR = new File(".");
//		LOGFILE = new File(LOGDIR, "loc_log.txt");
//		try {
//			WRLOGFILE = new FileWriter(LOGFILE);
//			BWRLOGFILE = new BufferedWriter(WRLOGFILE);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}

//		BENCH_START_TIME = System.currentTimeMillis();
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
		LOC_TABLE_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".LOC_TABLE_MAX_SIZE", -1);
		if (LOC_TABLE_MAX_SIZE == -1)
			locationTable = new HashMap<RecordKey, Integer>();
		else
			locationTable = new HashMap<RecordKey, Integer>(LOC_TABLE_MAX_SIZE + 1000);
		
//		new PeriodicalJob(3000, 500000, new Runnable() {
//			@Override
//			public void run() {
//				System.out.println("Location Table : " + locationTable.size() +
//						", Queue: " + fifoQueue.size());
//				}
//			}
//		).start();
		
		// Only for 4 nodes with micro-benchmarks
		// Note: Remember to use ConcurrentHashMap
//		new PeriodicalJob(3000, 500000, new Runnable() {
//			@Override
//			public void run() {
//				int[] counts = new int[4];
//				for (int i = 0; i < counts.length; i++)
//					counts[i] = 25_000;
//				
//				for (Entry<RecordKey, Integer> entry : locationTable.entrySet()) {
////					int id = Integer.parseInt((String) entry.getKey().getKeyVal("ycsb_id").asJavaVal());
//					int id = (Integer) entry.getKey().getKeyVal("i_id").asJavaVal();
//					int sourcePart = entry.getKey().hashCode() % 4;
//					int newPart = entry.getValue();
//					
//					if (id <= 100_000) {
//						counts[sourcePart]--;
//						counts[newPart]++;
//					}
//				}
//				
//				System.out.println(Arrays.toString(counts));
//			}
//		}).start();
		
//		Thread thread = new Thread(new Runnable() {
//			public void run() {
//				try {
//					Thread.sleep(450000);
//					File dir = new File(".");
//					File outputFile = new File(dir, "loc_tbl.txt");
//					FileWriter wrFile = new FileWriter(outputFile);
//					BufferedWriter bwrFile = new BufferedWriter(wrFile);
//					HashMap<RecordKey, LinkedList<Integer>> tmp = (HashMap<RecordKey, LinkedList<Integer>>) locationTable
//							.clone();
//
//					Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
//					RecordKey key;
//					int[] l;
//					int p, iid;
//					for (int j = 0; j < PartitionMetaMgr.NUM_PARTITIONS; j++) {
//						l = new int[PartitionMetaMgr.NUM_PARTITIONS];
//						System.out.print(String.format("Items : %7d ~ %7d -> ", j * 100000, (j + 1) * 100000));
//						for (int i = 1; i <= 100000; i++) {
//							iid = 100000 * j + i;
//							keyEntryMap.put("i_id", new IntegerConstant(iid));
//							key = new RecordKey("item", keyEntryMap);
//							LinkedList<Integer> t = tmp.get(key);
//
//							if (t != null)
//								p = t.getFirst();
//							else
//								p = getRangeLoc(key);
//							l[p]++;
//						}
//						for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
//							System.out.print(String.format("P %d : %6d ", i, l[i]));
//						System.out.println("");
//					}
//
//					System.out.println("Before");
//					for (int j = 0; j < PartitionMetaMgr.NUM_PARTITIONS; j++) {
//						l = new int[PartitionMetaMgr.NUM_PARTITIONS];
//						System.out.print(String.format("Items : %7d ~ %7d -> ", j * 100000, (j + 1) * 100000));
//						for (int i = 1; i <= 100000; i++) {
//							iid = 100000 * j + i;
//							keyEntryMap.put("i_id", new IntegerConstant(iid));
//							key = new RecordKey("item", keyEntryMap);
//
//							p = key.hashCode() % PartitionMetaMgr.NUM_PARTITIONS;
//							l[p]++;
//						}
//						for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
//							System.out.print(String.format("P %d : %6d ", i, l[i]));
//						System.out.println("");
//					}
//
//					for (Entry<RecordKey, LinkedList<Integer>> e : tmp.entrySet())
//						bwrFile.write(e.getKey() + " loc: " + e.getValue() + "\n");
//					bwrFile.close();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		});
//		thread.start();
	}
	
	public PartitionMetaMgr(PartitionPlan plan) {
		partPlan = plan;
	}

	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public boolean isFullyReplicated(RecordKey key) {
		return partPlan.isFullyReplicated(key);
	}
	
	/**
	 * Get the original location (may not be the current location)
	 * 
	 * @param key
	 * @return
	 */
	public int getPartition(RecordKey key) {
		return partPlan.getPartition(key);
	}
	
	public void startMigration(PartitionPlan newPlan) {
		partPlan = newPlan;
		isInMigration = true;
	}
	
	public void finishMigration() {
		isInMigration = false;
	}
	
	public PartitionPlan getPartitionPlan() {
		return partPlan;
	}
	
	public int getCurrentNumOfParts() {
		return partPlan.numberOfPartitions();
	}

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition where the record is
	 */
	public int getCurrentLocation(RecordKey key) {
		Integer partId = locationTable.get(key);
		if (partId != null)
			return partId;
		
		if (isInMigration) {
			partId = Elasql.migrationMgr().getLocation(key);
			if (partId != null)
				return partId;
		}
		
		return partPlan.getPartition(key);
	}

	public void setCurrentLocation(RecordKey key, int loc) {
//		try {
//			BWRLOGFILE.write((System.currentTimeMillis() - BENCH_START_TIME) + "," + key.getKeyVal("i_id") + "," + loc);
//			BWRLOGFILE.newLine();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		// If the new location matches the original partition, remove it from location table.
		boolean isInLocTable = locationTable.containsKey(key);
		if (isInLocTable && getPartition(key) == loc)
			locationTable.remove(key);
		else {
			if (LOC_TABLE_MAX_SIZE != -1) {
				if (PICKING_METHOD == PickingMethods.FIFO) {
					if (!isInLocTable) {
						fifoQueue.add(key);
					}
				}
			}
			
			locationTable.put(key, new Integer(loc));
		}
	}
	
	public Integer queryLocationTable(RecordKey key) {
		return locationTable.get(key);
	}
	
	public boolean removeFromLocationTable(RecordKey key) {
		return locationTable.remove(key) != null;
	}
	
	/**
	 * Choose the keys that should be removed next time.
	 * 
	 * @return
	 */
	public Set<RecordKey> chooseOverflowedKeys() {
		Set<RecordKey> removedKeys = new HashSet<RecordKey>();
		
		// If the limit is -1 (unlimited), return immediately.
		if (LOC_TABLE_MAX_SIZE == -1)
			return removedKeys;
		
		// Pick the keys that will be removed
		if (PICKING_METHOD == PickingMethods.FIFO) {
			while (locationTable.size() - removedKeys.size() > LOC_TABLE_MAX_SIZE) {
				RecordKey key = fifoQueue.remove();
				if (locationTable.containsKey(key))
					removedKeys.add(key);
			}
		}
		
		return removedKeys;
	}
}
