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

import org.elasql.sql.RecordKey;
import org.elasql.util.DDProperties;

public abstract class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;

	static {
		NUM_PARTITIONS = DDProperties.getLoader().getPropertyAsInteger(
				PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
	}

	// TODO: usage
	public abstract boolean isFullyReplicated(RecordKey key);

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 * @return the partition id
	 */
	public abstract int getPartition(RecordKey key);
}
