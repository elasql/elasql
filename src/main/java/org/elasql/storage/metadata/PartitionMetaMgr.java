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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;
import org.elasql.util.PeriodicalJob;

public class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;
//	public final static File LOGDIR;
//	public final static File LOGFILE;
	public static FileWriter WRLOGFILE;
	public static BufferedWriter BWRLOGFILE;
	
	public static final int LOC_TABLE_MAX_SIZE;
	private static FusionTable fusionTable;
	
	private PartitionPlan partPlan;
	private boolean isInMigration;

	static {
		
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
		LOC_TABLE_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".LOC_TABLE_MAX_SIZE", -1);
//		if (LOC_TABLE_MAX_SIZE == -1)
//			locationTable = new HashMap<RecordKey, Integer>();
//			locationTable = new ConcurrentHashMap<RecordKey, Integer>();
//		else
//			locationTable = new HashMap<RecordKey, Integer>(LOC_TABLE_MAX_SIZE + 1000);
		fusionTable = new FusionTable(LOC_TABLE_MAX_SIZE);
		
//		new PeriodicalJob(5000, 1500_000, new Runnable() {
//			@Override
//			public void run() {
//					System.out.println("Location Table : " + locationTable.size() +
//						", Queue: " + fifoQueue.size());
//					System.out.println("Fusion Table : " + fusionTable.size());
//				}
//			}
//		).start();
		
		// Note: Remember to use ConcurrentHashMap
//		new PeriodicalJob(5000, 660000, new Runnable() {
//			@Override
//			public void run() {
//				count(0);
//				count(1);
//			}
//			
//			void count(int tenantId) {
//				int startId = tenantId * 25_000 + 1;
//				int endId = (tenantId + 1) * 25_000;
//				int[] counts = new int[4];
//				for (int i = 0; i < counts.length; i++)
//					counts[i] = 6250;
//				
//				for (Entry<RecordKey, Integer> entry : locationTable.entrySet()) {
//					int id = Integer.parseInt((String) entry.getKey().getKeyVal("ycsb_id").asJavaVal());
////					int id = (Integer) entry.getKey().getKeyVal("i_id").asJavaVal();
//					int sourcePart = id % 4;
//					int newPart = entry.getValue();
//					
//					if (startId <= id && id <= endId) {
//						counts[sourcePart]--;
//						counts[newPart]++;
//					}
//				}
//				
//				System.out.println(String.format("Tenant %d: %s", tenantId, Arrays.toString(counts)));
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
		if (isInMigration) {
			Integer partId = Elasql.migrationMgr().getSourcePart(key);
			if (partId != null)
				return partId;
		}
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
		int location = fusionTable.getLocation(key);
		if (location != -1)
			return location;
		
		if (isInMigration) {
			Integer partId = Elasql.migrationMgr().getSourcePart(key);
			if (partId != null)
				return partId;
		}
		
		return partPlan.getPartition(key);
	}

	public void setCurrentLocation(RecordKey key, int loc) {
		if (getPartition(key) == loc && fusionTable.containsKey(key))
			fusionTable.remove(key);
		else
			fusionTable.setLocation(key, loc);
	}
	
	public Integer queryLocationTable(RecordKey key) {
		int partId = fusionTable.getLocation(key);
		if (partId == -1)
			return null;
		else
			return partId;
	}
	
	public boolean removeFromLocationTable(RecordKey key) {
		return fusionTable.remove(key) != -1;
	}
	
	/**
	 * Choose the keys that should be removed next time.
	 * 
	 * @return
	 */
	public Set<RecordKey> chooseOverflowedKeys() {
		return fusionTable.getOverflowKeys();
	}
}
