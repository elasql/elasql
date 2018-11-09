package org.elasql.migration;

import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.tx.Transaction;

public interface MigrationMgr {
	
	boolean USE_BYTES_FOR_CHUNK_SIZE = false;
	
	int CHUNK_SIZE_IN_BYTES = 1_000_000; // 1MB
	int CHUNK_SIZE_IN_COUNT = 15000;
	int CHUNK_SIZE = USE_BYTES_FOR_CHUNK_SIZE? CHUNK_SIZE_IN_BYTES : CHUNK_SIZE_IN_COUNT;
	
	void initializeMigration(Transaction tx, Object[] params);
	
	void finishMigration(Transaction tx, Object[] params);
	
	boolean isMigratingRecord(RecordKey key);
	
	boolean isMigrated(RecordKey key);
	
	void setMigrated(RecordKey key);
	
	int checkSourceNode(RecordKey key);
	
	int checkDestNode(RecordKey key);
	
	boolean isInMigration();
	
	ReadWriteSetAnalyzer newAnalyzer();
	
}
