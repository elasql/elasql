package org.elasql.migration.planner.clay;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.PrimaryKey;

public class ScatterMigrationRange implements MigrationRange {
	
	private int sourcePartId, destPartId;
	
	private Set<PrimaryKey> targetKeys;
	private Set<PrimaryKey> unmigratedKeys;
	private Deque<PrimaryKey> keysToPush;
	
	public ScatterMigrationRange(int sourcePartId, int destPartId, Set<PrimaryKey> keys) {
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
		this.targetKeys = keys;
		this.unmigratedKeys = new HashSet<PrimaryKey>(targetKeys);
		this.keysToPush = new ArrayDeque<PrimaryKey>(targetKeys);
	}

	@Override
	public boolean addKey(PrimaryKey key) {
		throw new UnsupportedOperationException("A scatter migration range does not support adding keys");
	}

	@Override
	public boolean contains(PrimaryKey key) {
		return targetKeys.contains(key);
	}

	@Override
	public boolean isMigrated(PrimaryKey key) {
		return !unmigratedKeys.contains(key);
	}

	@Override
	public void setMigrated(PrimaryKey key) {
		if (targetKeys.contains(key))
			unmigratedKeys.remove(key);
	}

	@Override
	public Set<PrimaryKey> generateNextMigrationChunk(boolean useBytesForSize, int maxChunkSize) {
		if (useBytesForSize)
			throw new UnsupportedOperationException("A scatter migration range does not support byte counting");
		
		Set<PrimaryKey> chunk = new HashSet<PrimaryKey>();
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
				new ArrayDeque<PrimaryKey>(keysToPush));
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
