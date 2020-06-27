package org.elasql.migration.planner;

import java.util.Set;

import org.elasql.migration.MigrationPlan;
import org.elasql.sql.RecordKey;

public interface MigrationPlanner {
	
	void monitorTransaction(Set<RecordKey> reads, Set<RecordKey> writes);
	
	MigrationPlan generateMigrationPlan();
	
	void reset();
	
}
