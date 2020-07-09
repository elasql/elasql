package org.elasql.migration;

import org.elasql.sql.RecordKey;

public class DummyMigrationComponentFactory extends MigrationComponentFactory {
	
	private String message;
	
	public DummyMigrationComponentFactory(String message) {
		this.message = message;
	}

	@Override
	public MigrationPlan newPredefinedMigrationPlan() {
		throw new RuntimeException(message);
	}

	@Override
	public MigrationRange toMigrationRange(int sourceId, int destId, RecordKey partitioningKey) {
		throw new RuntimeException(message);
	}

}
