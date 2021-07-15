/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.storage.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.comm.server.VanillaCommServer;

public class PartitionMetaMgr {
	private static Logger logger = Logger.getLogger(PartitionMetaMgr.class.getName());

	public static final int NUM_PARTITIONS;

	static {
		if (Elasql.ENABLE_STAND_ALONE_SEQUENCER) {
			NUM_PARTITIONS = VanillaCommServer.getServerCount() - 1;
		} else {
			NUM_PARTITIONS = VanillaCommServer.getServerCount();
		}
	}

	private PartitionPlan partPlan;
	
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
	public boolean isFullyReplicated(PrimaryKey key) {
		return partPlan.isFullyReplicated(key);
	}
	
	/**
	 * Get the original location (may not be the current location)
	 * 
	 * @param key
	 * @return
	 */
	public int getPartition(PrimaryKey key) {
		return partPlan.getPartition(key);
	}
	
	public void setNewPartitionPlan(PartitionPlan newPlan) {
		// XXX: Bug: If there is a plan warping another plan,
		// this may make the warping plan disappear.
		partPlan = newPlan;
	}
	
	public PartitionPlan getPartitionPlan() {
		return partPlan;
	}
	
	public int getCurrentNumOfParts() {
		return partPlan.numberOfPartitions();
	}
	
	public PartitioningKey getPartitioningKey(PrimaryKey key) {
		return partPlan.getPartitioningKey(key);
	}
}
