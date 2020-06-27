package org.elasql.migration.planner;

import java.util.Set;

import org.elasql.migration.MigrationPlan;
import org.elasql.sql.RecordKey;

public class PredefinedPlanner implements MigrationPlanner {
	
	private MigrationPlan predefinedPlan;
	
	public PredefinedPlanner(MigrationPlan plan) {
		this.predefinedPlan = plan;
	}

	@Override
	public void monitorTransaction(Set<RecordKey> reads, Set<RecordKey> writes) {
		// do nothing
	}

	@Override
	public MigrationPlan generateMigrationPlan() {
		return predefinedPlan;
	}

	@Override
	public void reset() {
		// do nothing
	}
}
