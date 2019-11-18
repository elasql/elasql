package org.elasql.migration.tpart.sp;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.migration.MigrationRange;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

/**
 * This transaction migrates cold data in the migration range. The cold data
 * are the records that are not in the fusion table. This transaction must
 * be processed and sunk individually. If it was processed and sunk with other
 * transactions by T-Part, some records might become missing.
 * 
 * @author yslin
 *
 */
public class ColdMigrationProcedure extends TPartStoredProcedure<ColdMigrationParamHelper> {
	private static Logger logger = Logger.getLogger(ColdMigrationProcedure.class.getName());
	
	// the tuples that are not moved
	private Set<RecordKey> coldKeys = new HashSet<RecordKey>();
	
	public ColdMigrationProcedure(long txNum) {
		super(txNum, new ColdMigrationParamHelper());
	}

	@Override
	protected void prepareKeys() {
		MigrationRange range = paramHelper.getMigrationRange();
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		
		// Iterate over the keys in the migration range
		Iterator<RecordKey> keyIter = Elasql.migrationMgr().toKeyIterator(range);
		while (keyIter.hasNext()) {
			RecordKey key = keyIter.next();
			
			// Check if the record is in the fusion table (cache)
			// We only move the records not in the table,
			// which we consider as cold data.
			// -------------------------------------------------
			// Note that some records are in the fusion table
			// but currently located in the destination node.
			// We ignore those records for now, but later txs
			// who touch them should insert them to the storage
			// of the destination node.
			// -------------------------------------------------
			Integer partId = partMgr.queryLocationTable(key);
			if (partId == null)
				coldKeys.add(key);
		}
		
//		System.out.println("Cold keys: " + coldKeys);
		
		// Add to read & write keys
		for (RecordKey key : coldKeys) {
			addReadKey(key);
			addWriteKey(key);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Tx." + txNum + " starts a cold migration");
		
		// Do nothing
	}

	@Override
	public double getWeight() {
		return coldKeys.size();
	}
	
	@Override
	public ProcedureType getProcedureType() {
		return ProcedureType.MIGRATION;
	}
	
	public MigrationRange getMigrationRange() {
		return paramHelper.getMigrationRange();
	}
}
