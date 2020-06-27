package org.elasql.migration.planner.clay;

import java.util.Set;

import org.elasql.migration.MigrationPlan;
import org.elasql.migration.planner.MigrationPlanner;
import org.elasql.sql.RecordKey;

public class ClayPlanner implements MigrationPlanner {

	@Override
	public void monitorTransaction(Set<RecordKey> reads, Set<RecordKey> writes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MigrationPlan generateMigrationPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}
