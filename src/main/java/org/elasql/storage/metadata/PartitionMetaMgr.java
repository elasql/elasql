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

import java.util.HashMap;

import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;
import org.elasql.util.PeriodicalJob;

public abstract class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;
	
	private static HashMap<RecordKey,Integer> locationTable;

	static {
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
		locationTable = new HashMap<RecordKey,Integer>();
		new PeriodicalJob(3000, 500000, new Runnable() {
			@Override
			 public void run(){
				System.out.println("loc_tbl : "+locationTable.size());
			}
		}).start();
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
	public int getPartition(RecordKey key){
		Integer loc = locationTable.get(key);
		if(loc == null)
			return getLocation(key);
		else
			return loc;
	}
	public Integer setPartition(RecordKey key , int loc){
		return locationTable.put(key, new Integer(loc));
	}
	
	protected abstract int getLocation(RecordKey key);
	
	
		
	
}
