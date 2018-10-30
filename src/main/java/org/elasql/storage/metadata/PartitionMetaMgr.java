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

import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;

public class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;

	static {
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
	}

	private PartitionPlan partPlan;
	
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
	
	public PartitionPlan getPartitionPlan() {
		return partPlan;
	}
	
	public int getCurrentNumOfParts() {
		return partPlan.numberOfPartitions();
	}
}
