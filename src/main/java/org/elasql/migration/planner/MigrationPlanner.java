package org.elasql.migration.planner;

import java.util.Set;

import org.elasql.migration.MigrationPlan;
import org.elasql.sql.PrimaryKey;

public interface MigrationPlanner {
	
	void monitorTransaction(Set<PrimaryKey> reads, Set<PrimaryKey> writes);
	
	MigrationPlan generateMigrationPlan();
	
	void reset();
	
}
