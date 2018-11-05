package org.elasql.storage.metadata;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class NotificationPartitionPlan extends PartitionPlan {
	
	public static final String TABLE_NAME = "notification";
	public static final String KEY_SENDER_NAME = "sender_node_id";
	public static final String KEY_RECV_NAME = "recv_node_id";
	
	public static RecordKey createRecordKey(int sender, int reciever) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put(KEY_SENDER_NAME, new IntegerConstant(sender));
		keyEntryMap.put(KEY_RECV_NAME, new IntegerConstant(reciever));
		return new RecordKey(TABLE_NAME, keyEntryMap);
	}
	
	public static CachedRecord createRecord(int sender, int reciever, long txNum) {
		return createRecord(sender, reciever, txNum, new HashMap<String, Constant>());
	}
	
	public static CachedRecord createRecord(int sender, int reciever,
			long txNum, Map<String, Constant> fldVals) {
		// Create key value sets
		Map<String, Constant> newFldVals = new HashMap<String, Constant>(fldVals);
		newFldVals.put(KEY_SENDER_NAME, new IntegerConstant(sender));
		newFldVals.put(KEY_RECV_NAME, new IntegerConstant(reciever));

		// Create a record
		CachedRecord rec = new CachedRecord(newFldVals);
		rec.setSrcTxNum(txNum);
		return rec;
	}
	
	private PartitionPlan underlayerPlan;
	
	public NotificationPartitionPlan(PartitionPlan plan) {
		underlayerPlan = plan;
	}
	
	@Override
	public boolean isFullyReplicated(RecordKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return false;
		
		return underlayerPlan.isFullyReplicated(key);
	}

	@Override
	public int getPartition(RecordKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return -1; // Not belongs to anyone, preventing for inserting to local
		
		return underlayerPlan.getPartition(key);
	}
	
	@Override
	public int numberOfPartitions() {
		return underlayerPlan.numberOfPartitions();
	}
	
	public PartitionPlan getUnderlayerPlan() {
		return underlayerPlan;
	}
	
	@Override
	public String toString() {
		return String.format("Notification Partition Plan (underlayer: %s)", underlayerPlan.toString());
	}
}
