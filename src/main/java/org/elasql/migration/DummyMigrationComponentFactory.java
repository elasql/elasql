package org.elasql.migration;

public class DummyMigrationComponentFactory extends MigrationComponentFactory {
	
	private String message;
	
	public DummyMigrationComponentFactory(String message) {
		this.message = message;
	}

	@Override
	public MigrationPlan newPredefinedMigrationPlan() {
		throw new RuntimeException(message);
	}

}
