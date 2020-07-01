package org.elasql.migration.planner.clay;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;

public class ScatterMigrationRangeUpdate implements MigrationRangeUpdate {
	
	private static final long serialVersionUID = 20181101001L;

	private int sourcePartId, destPartId;
	private Set<RecordKey> unmigratedKeys = new HashSet<RecordKey>();
	
	ScatterMigrationRangeUpdate(int sourcePartId, int destPartId,
			Set<RecordKey> unmigratedKeys) {
		this.unmigratedKeys = unmigratedKeys;
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
	
	Set<RecordKey> getUnmigratedKeys() {
		return unmigratedKeys;
	}
}
