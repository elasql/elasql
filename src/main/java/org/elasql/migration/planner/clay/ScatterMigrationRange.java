package org.elasql.migration.planner.clay;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;

public class ScatterMigrationRange implements MigrationRange {
	
	private Set<RecordKey> targetKeys;
	private int sourcePartId, destPartId;
	private Set<RecordKey> unmigratedKeys = new HashSet<RecordKey>();
	
	public ScatterMigrationRange(int sourcePartId, int destPartId, Set<RecordKey> keys) {
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
		this.targetKeys = keys;
	}

	@Override
	public boolean addKey(RecordKey key) {
		throw new UnsupportedOperationException("A scatter migration range does not support adding keys");
	}

	@Override
	public boolean contains(RecordKey key) {
		return targetKeys.contains(key);
	}

	@Override
	public boolean isMigrated(RecordKey key) {
		return !unmigratedKeys.contains(key);
	}

	@Override
	public void setMigrated(RecordKey key) {
		if (targetKeys.contains(key))
			unmigratedKeys.remove(key);
	}

	@Override
	public Set<RecordKey> generateNextMigrationChunk(boolean useBytesForSize, int maxChunkSize) {
		if (useBytesForSize)
			throw new UnsupportedOperationException("A scatter migration range does not support byte counting");
		
		Set<RecordKey> chunk = new HashSet<RecordKey>();
		for (RecordKey key : unmigratedKeys) {
			if (chunk.size() >= maxChunkSize)
				break;
			chunk.add(key);
		}
		
		return chunk;
	}

	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}

	@Override
	public MigrationRangeUpdate generateStatusUpdate() {
		return new ScatterMigrationRangeUpdate(sourcePartId, destPartId,
				new HashSet<RecordKey>(unmigratedKeys));
	}

	@Override
	public boolean updateMigrationStatus(MigrationRangeUpdate update) {
		ScatterMigrationRangeUpdate sUpdate = (ScatterMigrationRangeUpdate) update;
		unmigratedKeys = sUpdate.getUnmigratedKeys();
		return true;
	}

}
