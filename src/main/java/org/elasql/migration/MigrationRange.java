package org.elasql.migration;

import java.util.Set;

import org.elasql.sql.RecordKey;

public interface MigrationRange {
	
	boolean addKey(RecordKey key);
	
	boolean contains(RecordKey key);
	
	boolean isMigrated(RecordKey key);
	
	void setMigrated(RecordKey key);
	
	Set<RecordKey> generateNextMigrationChunk(boolean useBytesForSize, int maxChunkSize);
	
	int getSourcePartId();
	
	int getDestPartId();
	
	MigrationRangeUpdate generateStatusUpdate();
	
	boolean updateMigrationStatus(MigrationRangeUpdate update);
}
