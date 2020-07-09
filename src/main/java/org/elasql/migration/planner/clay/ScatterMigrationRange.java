package org.elasql.migration.planner.clay;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;

public class ScatterMigrationRange implements MigrationRange {
	
	private int sourcePartId, destPartId;
	
	private Set<RecordKey> targetKeys;
	private Set<RecordKey> unmigratedKeys;
	private Deque<RecordKey> keysToPush;
	
	public ScatterMigrationRange(int sourcePartId, int destPartId, Set<RecordKey> keys) {
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
		this.targetKeys = keys;
		this.unmigratedKeys = new HashSet<RecordKey>(targetKeys);
		this.keysToPush = new ArrayDeque<RecordKey>(targetKeys);
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
		while (!keysToPush.isEmpty()) {
			chunk.add(keysToPush.removeFirst());
			if (chunk.size() >= maxChunkSize)
				break;
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
				new ArrayDeque<RecordKey>(keysToPush));
	}

	@Override
	public boolean updateMigrationStatus(MigrationRangeUpdate update) {
		ScatterMigrationRangeUpdate sUpdate = (ScatterMigrationRangeUpdate) update;
		// Only keeps the keys that are will be pushed in the future.
		// The other keys that are not in the keysToPush must have been migrated
		// by either foreground pushes/pulls or background pushes.
		unmigratedKeys.retainAll(sUpdate.getKeysToPush());
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("Migration from part.%d to part.%d with %d keys [sample: %s]",
				sourcePartId, destPartId, targetKeys.size(), targetKeys.iterator().next());
	}

}
