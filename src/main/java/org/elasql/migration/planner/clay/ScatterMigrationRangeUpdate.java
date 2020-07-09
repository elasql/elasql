package org.elasql.migration.planner.clay;

import java.util.Deque;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;

public class ScatterMigrationRangeUpdate implements MigrationRangeUpdate {
	
	private static final long serialVersionUID = 20181101001L;

	private int sourcePartId, destPartId;
	private Deque<RecordKey> keysToPush;
	
	ScatterMigrationRangeUpdate(int sourcePartId, int destPartId,
			Deque<RecordKey> keysToPush) {
		this.keysToPush = keysToPush;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
	}
	
	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}
	
	Deque<RecordKey> getKeysToPush() {
		return keysToPush;
	}
}
