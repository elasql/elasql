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

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;

public class PartitionMetaMgr {
	private static Logger logger = Logger.getLogger(PartitionMetaMgr.class.getName());

	public static final int NUM_PARTITIONS;
	public static final int LOC_TABLE_MAX_SIZE;
	
	private static final FusionTable FUSION_TABLE;
	
	private PartitionPlan partPlan;
	private boolean isInMigration;

	static {
		
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
		LOC_TABLE_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".LOC_TABLE_MAX_SIZE", -1);
		FUSION_TABLE = new FusionTable(LOC_TABLE_MAX_SIZE);
	}
	
	public PartitionMetaMgr(PartitionPlan plan) {
		partPlan = plan;
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Using '%s'", partPlan));
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
			Integer partId = Elasql.migrationMgr().getPartition(key);
			if (partId != null)
				return partId;
		}
		return partPlan.getPartition(key);
	}
	
	public void startMigration(PartitionPlan newPlan) {
		partPlan.changeBasePartitionPlan(newPlan);
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
		int location = FUSION_TABLE.getLocation(key);
		if (location != -1)
			return location;
		
		if (isInMigration) {
			Integer partId = Elasql.migrationMgr().getPartition(key);
			if (partId != null)
				return partId;
		}
		
		return partPlan.getPartition(key);
	}

	public void setCurrentLocation(RecordKey key, int loc) {
		if (getPartition(key) == loc && FUSION_TABLE.containsKey(key))
			FUSION_TABLE.remove(key);
		else
			FUSION_TABLE.setLocation(key, loc);
	}
	
	public Integer queryLocationTable(RecordKey key) {
		int partId = FUSION_TABLE.getLocation(key);
		if (partId == -1)
			return null;
		else
			return partId;
	}
	
	public boolean removeFromLocationTable(RecordKey key) {
		return FUSION_TABLE.remove(key) != -1;
	}
	
	/**
	 * Choose the keys that should be removed next time.
	 * 
	 * @return
	 */
	public Set<RecordKey> chooseOverflowedKeys() {
		return FUSION_TABLE.getOverflowKeys();
	}
}
