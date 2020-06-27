package org.elasql.migration;

import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.tx.Transaction;

public interface MigrationMgr {
	
	void initializeMigration(Transaction tx, MigrationPlan plan, Object[] params);
	
	void finishMigration(Transaction tx, Object[] params);
	
	boolean isMigratingRecord(RecordKey key);
	
	boolean isMigrated(RecordKey key);
	
	void setMigrated(RecordKey key);
	
	int checkSourceNode(RecordKey key);
	
	int checkDestNode(RecordKey key);
	
	boolean isInMigration();
	
	ReadWriteSetAnalyzer newAnalyzer();
}
