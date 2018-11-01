package org.elasql.migration;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.migration.CrabbingAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;

/**
 * The migration manager that exists in each node. Job: 
 * (1) trace the migration states, 
 * (2) initialize a background push transaction, and 
 * (3) send the finish notification to the main controller on the sequencer node.
 */
public abstract class MigrationMgr {
	private static Logger logger = Logger.getLogger(MigrationMgr.class.getName());
	
	private Phase currentPhase = Phase.NORMAL;
	private List<MigrationRange> migrationRanges;
	private PartitionPlan newPartitionPlan;
	
	public void initializeMigration(PartitionPlan newPartPlan, Phase initialPhase) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - Elasql.SYSTEM_INIT_TIME_MS;
			logger.info(String.format("a new migration starts at %d. New Plan: %s"
					, time / 1000, newPartPlan));
		}
		
		currentPhase = initialPhase;
		migrationRanges = generateMigrationRanges(newPartPlan);
		newPartitionPlan = newPartPlan;
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
		}
	}
	
	public void finishMigration() {
		// TODO
		
		// Change the current partition plan of the system
//		Elasql.partitionMetaMgr().startMigration(newPartitionPlan);
	}
	
	public boolean isMigratingRecord(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return true;
		return false;
	}
	
	public boolean isMigrated(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.isMigrated(key);
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public void setMigrated(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key)) {
				range.setMigrated(key);
				return;
			}
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkSourceNode(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getSourcePartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkDestNode(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getDestPartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public Phase getCurrentPhase() {
		return currentPhase;
	}
	
	public boolean isInMigration() {
		return currentPhase != Phase.NORMAL;
	}
	
	public ReadWriteSetAnalyzer newAnalyzer() {
		return new CrabbingAnalyzer();
	}
	
	public abstract List<MigrationRange> generateMigrationRanges(PartitionPlan newPlan);
}
