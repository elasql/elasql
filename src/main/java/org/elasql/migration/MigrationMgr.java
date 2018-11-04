package org.elasql.migration;

import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.RecordKey;

public interface MigrationMgr {
	
	int CHUNK_SIZE = 1_000_000; // 1MB
	
	void initializeMigration(Object[] params);
	
	void finishMigration(Object[] params);
	
	boolean isMigratingRecord(RecordKey key);
	
	boolean isMigrated(RecordKey key);
	
	void setMigrated(RecordKey key);
	
	int checkSourceNode(RecordKey key);
	
	int checkDestNode(RecordKey key);
	
	boolean isInMigration();
	
	ReadWriteSetAnalyzer newAnalyzer();
	
}
