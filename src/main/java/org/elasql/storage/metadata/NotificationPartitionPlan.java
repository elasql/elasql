package org.elasql.storage.metadata;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class NotificationPartitionPlan extends PartitionPlan {
	
	public static final String TABLE_NAME = "notification";
	public static final String KEY_SENDER_NAME = "sender_node_id";
	public static final String KEY_RECV_NAME = "recv_node_id";
	
	public static PrimaryKey createRecordKey(int sender, int reciever) {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder(TABLE_NAME);
		builder.addFldVal(KEY_SENDER_NAME, new IntegerConstant(sender));
		builder.addFldVal(KEY_RECV_NAME, new IntegerConstant(reciever));
		return builder.build();
	}
	
	public static CachedRecord createRecord(int sender, int reciever, long txNum) {
		// Create a record
		PrimaryKey key = createRecordKey(sender, reciever);
		CachedRecord rec = new CachedRecord(key);
		rec.setSrcTxNum(txNum);
		rec.setTempRecord(true);
		return rec;
	}
	
	private PartitionPlan basePlan;
	
	public NotificationPartitionPlan(PartitionPlan plan) {
		basePlan = plan;
	}
	
	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return false;
		
		return basePlan.isFullyReplicated(key);
	}

	@Override
	public int getPartition(PrimaryKey key) {
		if (key.getTableName().equals(TABLE_NAME))
			return -1; // Not belongs to anyone, preventing for inserting to local
		
		return basePlan.getPartition(key);
	}
	
	@Override
	public int numberOfPartitions() {
		return basePlan.numberOfPartitions();
	}
	
	@Override
	public String toString() {
		return String.format("Notification Partition Plan (underlayer: %s)", basePlan.toString());
	}

	@Override
	public PartitionPlan getBasePlan() {
		return basePlan;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		basePlan = plan;
	}

	@Override
	public PartitioningKey getPartitioningKey(PrimaryKey key) {
		return basePlan.getPartitioningKey(key);
	}
}
