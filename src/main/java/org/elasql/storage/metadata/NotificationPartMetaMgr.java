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
	public int getLocation(RecordKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return -1; // Not belongs to anyone, preventing for inserting to local
		
		return underliedPartMetaMgr.getLocation(key);
	}

}
