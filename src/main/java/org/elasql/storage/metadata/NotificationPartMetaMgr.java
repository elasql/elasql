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

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class NotificationPartMetaMgr extends PartitionMetaMgr {
	
	public static final String TABLE_NAME = "notification";
	public static final String KEY_SOURCE_NAME = "src_server_id";
	public static final String KEY_DEST_NAME = "dest_server_id";
	
	public static RecordKey createRecordKey(int srcNodeId, int destNodeId) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put(KEY_SOURCE_NAME, new IntegerConstant(srcNodeId));
		keyEntryMap.put(KEY_DEST_NAME, new IntegerConstant(destNodeId));
		return new RecordKey(TABLE_NAME, keyEntryMap);
	}
	
	public static CachedRecord createRecord(int srcNodeId, int destNodeId,
			long txNum, Map<String, Constant> fldVals) {
		// Create key value sets
		Map<String, Constant> newFldVals = new HashMap<String, Constant>(fldVals);
		newFldVals.put(KEY_SOURCE_NAME, new IntegerConstant(srcNodeId));
		newFldVals.put(KEY_DEST_NAME, new IntegerConstant(destNodeId));

		// Create a record
		CachedRecord rec = new CachedRecord(newFldVals);
		rec.setSrcTxNum(txNum);
		return rec;
	}
	
	private PartitionMetaMgr underliedPartMetaMgr;
	
	public NotificationPartMetaMgr(PartitionMetaMgr partMetaMgr) {
		underliedPartMetaMgr = partMetaMgr;
	}
	
	@Override
	public boolean isFullyReplicated(RecordKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return false;
		
		return underliedPartMetaMgr.isFullyReplicated(key);
	}

	@Override
	public int getPartition(RecordKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return -1; // Not belongs to anyone, preventing for inserting to local
		
		return underliedPartMetaMgr.getPartition(key);
	}

}
