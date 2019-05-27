package org.elasql.storage.metadata;

import java.util.ArrayList;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class NotificationPartitionPlan extends PartitionPlan {
	
	public static final String TABLE_NAME = "notification";
	public static final String KEY_SOURCE_NAME = "src_server_id";
	public static final String KEY_DEST_NAME = "dest_server_id";
	
	public static RecordKey createRecordKey(int srcNodeId, int destNodeId) {
		ArrayList<String> fields = new ArrayList<String>();
		fields.add(KEY_SOURCE_NAME);
		fields.add(KEY_DEST_NAME);
		
		ArrayList<Constant> vals = new ArrayList<Constant>();
		vals.add(new IntegerConstant(srcNodeId));
		vals.add(new IntegerConstant(destNodeId));
		
		return new RecordKey(TABLE_NAME, fields, vals);
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
		return String.format("NotificationPlan: [%s]", underlayerPlan.toString());
	}
}
