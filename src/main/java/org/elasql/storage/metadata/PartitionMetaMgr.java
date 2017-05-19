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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;
import org.elasql.util.PeriodicalJob;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public abstract class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;
/*
	private class Tuple<X> {
		public X loc;
		public int times;

		public Tuple(X loc) {
			this.loc = loc;
			this.times = 0;
		}

		public void encrease() {
			this.times++;
		}

		public void setLot(X loc) {
			this.loc = loc;
		}
	}
*/
	private static HashMap<RecordKey, LinkedList<Integer>> locationTable;

	static {

		
		
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
		locationTable = new HashMap<RecordKey, LinkedList<Integer>>();
		/*
		 * new PeriodicalJob(3000, 500000, new Runnable() {
		 * 
		 * @Override public void run() { System.out.println("loc_tbl : " +
		 * locationTable.size()); } }).start();
		 */
		Thread thread = new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(850000);
					File dir = new File(".");
					File outputFile = new File(dir, "loc_tbl.txt");
					FileWriter wrFile = new FileWriter(outputFile);
					BufferedWriter bwrFile = new BufferedWriter(wrFile);
					HashMap<RecordKey, LinkedList<Integer>> tmp = (HashMap<RecordKey, LinkedList<Integer>>) locationTable.clone();

					Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
					RecordKey key;
					int[] l;
					int p, iid;
					for (int j = 0; j < PartitionMetaMgr.NUM_PARTITIONS; j++) {
						l = new int[PartitionMetaMgr.NUM_PARTITIONS];
						System.out.print(String.format("Items : %7d ~ %7d -> ", j * 100000, (j + 1) * 100000));
						for (int i = 1; i <= 100000; i++) {
							iid = 100000 * j + i;
							keyEntryMap.put("i_id", new IntegerConstant(iid));
							key = new RecordKey("item", keyEntryMap);
							LinkedList<Integer> t = tmp.get(key);

							if (t != null)
								p = t.getFirst();
							else
								p = key.hashCode() % PartitionMetaMgr.NUM_PARTITIONS;
							l[p]++;
						}
						for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
							System.out.print(String.format("P %d : %6d ", i, l[i]));
						System.out.println("");
					}

					System.out.println("Before");
					for (int j = 0; j < PartitionMetaMgr.NUM_PARTITIONS; j++) {
						l = new int[PartitionMetaMgr.NUM_PARTITIONS];
						System.out.print(String.format("Items : %7d ~ %7d -> ", j * 100000, (j + 1) * 100000));
						for (int i = 1; i <= 100000; i++) {
							iid = 100000 * j + i;
							keyEntryMap.put("i_id", new IntegerConstant(iid));
							key = new RecordKey("item", keyEntryMap);
						

							p = key.hashCode() % PartitionMetaMgr.NUM_PARTITIONS;
							l[p]++;
						}
						for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
							System.out.print(String.format("P %d : %6d ", i, l[i]));
						System.out.println("");
					}

					for (Entry<RecordKey, LinkedList<Integer>> e : tmp.entrySet())
						bwrFile.write(e.getKey() + " loc: " + e.getValue() + "\n");
					bwrFile.close();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		thread.start();
	}

	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public abstract boolean isFullyReplicated(RecordKey key);

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition where the record is
	 */
	public int getPartition(RecordKey key) {
		LinkedList<Integer> old = locationTable.get(key);
		if (old == null)
			return getLocation(key);
		else
			return old.getFirst();
	}

	public void setPartition(RecordKey key, int loc) {

		LinkedList<Integer> old = locationTable.get(key);
		if (old == null){
			LinkedList<Integer> l = new LinkedList<Integer>();
			l.addFirst(loc);
			locationTable.put(key, l);
		}
		else 
			old.set(0, loc);
		
	}

	protected abstract int getLocation(RecordKey key);

}
