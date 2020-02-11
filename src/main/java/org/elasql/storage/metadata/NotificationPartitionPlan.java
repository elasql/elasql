package org.elasql.storage.metadata;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.RecordKey;
import org.elasql.sql.RecordKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class NotificationPartitionPlan extends PartitionPlan {
	
	public static final String TABLE_NAME = "notification";
	public static final String KEY_SENDER_NAME = "sender_node_id";
	public static final String KEY_RECV_NAME = "recv_node_id";
	
	public static RecordKey createRecordKey(int sender, int reciever) {
		RecordKeyBuilder builder = new RecordKeyBuilder(TABLE_NAME);
		builder.addFldVal(KEY_SENDER_NAME, new IntegerConstant(sender));
		builder.addFldVal(KEY_RECV_NAME, new IntegerConstant(reciever));
		return builder.build();
	}
	
	public static CachedRecord createRecord(int sender, int reciever, long txNum) {
		// Create a record
		RecordKey key = createRecordKey(sender, reciever);
		CachedRecord rec = new CachedRecord(key);
		rec.setSrcTxNum(txNum);
		rec.setTempRecord(true);
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
