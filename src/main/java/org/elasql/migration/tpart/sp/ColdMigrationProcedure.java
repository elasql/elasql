package org.elasql.migration.tpart.sp;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.elasql.cache.CachedRecord;
import org.elasql.migration.MigrationRange;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ColdMigrationProcedure extends TPartStoredProcedure<ColdMigrationParamHelper> {
	
	// the keys that are not moved
	private Set<RecordKey> coldKeys = new HashSet<RecordKey>(); 
	
	// the keys that are moved to the dest node by other txs
	// these need to be deleted from cache and inserted to the local storage
	private Set<RecordKey> localInsertKeys = new HashSet<RecordKey>(); 
	
	public ColdMigrationProcedure(long txNum) {
		super(txNum, new ColdMigrationParamHelper());
	}

	@Override
	protected void prepareKeys() {
		MigrationRange range = paramHelper.getMigrationRange();
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		
		// Iterate over the keys in the migration range
		Iterator<RecordKey> keyIter = Elasql.migrationMgr().getKeyIterator(range);
		while (keyIter.hasNext()) {
			RecordKey key = keyIter.next();
			
			// Check if it has been moved
			Integer partId = partMgr.queryLocationTable(key);
			
			if (partId == null) {
				coldKeys.add(key);
			} else if (partId == range.getDestPartId()) {
				localInsertKeys.add(key);
			}
		}
		
		// Add to read & write keys
		for (RecordKey key : coldKeys) {
			addReadKey(key);
			addWriteKey(key);
		}
		for (RecordKey key : localInsertKeys) {
			addReadKey(key);
			addWriteKey(key);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// Do nothing
	}

	@Override
	public double getWeight() {
		return coldKeys.size() + localInsertKeys.size();
	}
	
	@Override
	public ProcedureType getProcedureType() {
		return ProcedureType.MIGRATION;
	}
	
	public Set<RecordKey> getLocalCacheToStorage() {
		return localInsertKeys;
	}
	
	public MigrationRange getMigrationRange() {
		return paramHelper.getMigrationRange();
	}
}
