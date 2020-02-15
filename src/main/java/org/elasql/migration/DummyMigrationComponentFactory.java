package org.elasql.migration;

import java.util.List;

import org.elasql.storage.metadata.PartitionPlan;

public class DummyMigrationComponentFactory extends MigrationComponentFactory {
	
	private String message;
	
	public DummyMigrationComponentFactory(String message) {
		this.message = message;
	}

	@Override
	public List<MigrationRange> generateMigrationRanges(PartitionPlan oldPlan, PartitionPlan newPlan) {
		throw new RuntimeException(message);
	}

	@Override
	public PartitionPlan newPartitionPlan() {
		throw new RuntimeException(message);
	}

}
