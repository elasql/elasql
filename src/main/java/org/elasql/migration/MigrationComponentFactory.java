package org.elasql.migration;

import java.util.List;

import org.elasql.migration.mgcrab.MgCrabMigrationMgr;
import org.elasql.migration.mgcrab.MgCrabStoredProcFactory;
import org.elasql.migration.mgcrab.MgCrabSystemController;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.storage.metadata.PartitionPlan;

public abstract class MigrationComponentFactory {
	
	public static final MigrationAlgorithm CURRENT_ALGO = MigrationAlgorithm.MGCRAB;
	
	public MigrationMgr newMigrationMgr() {
		switch (CURRENT_ALGO) {
		case MGCRAB:
			return new MgCrabMigrationMgr(this);
		case SQUALL:
			throw new RuntimeException("haven't implement for Squall yet");
		case STOP_COPY:
			throw new RuntimeException("haven't implement for Squall yet");
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationSystemController newSystemController() {
		switch (CURRENT_ALGO) {
		case MGCRAB:
			return new MgCrabSystemController(this);
		case SQUALL:
			throw new RuntimeException("haven't implement for Squall yet");
		case STOP_COPY:
			throw new RuntimeException("haven't implement for Squall yet");
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationStoredProcFactory newMigrationSpFactory(
			CalvinStoredProcedureFactory underlayerFactory) {
		switch (CURRENT_ALGO) {
		case MGCRAB:
			return new MgCrabStoredProcFactory(underlayerFactory);
		case SQUALL:
			throw new RuntimeException("haven't implement for Squall yet");
		case STOP_COPY:
			throw new RuntimeException("haven't implement for Squall yet");
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public abstract List<MigrationRange> generateMigrationRanges(PartitionPlan newPlan);
	
	public abstract PartitionPlan newPartitionPlan();
}
