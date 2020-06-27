package org.elasql.migration;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.albatross.AlbatrossMigrationMgr;
import org.elasql.migration.albatross.AlbatrossStoredProcFactory;
import org.elasql.migration.mgcrab.MgCrabMigrationMgr;
import org.elasql.migration.mgcrab.MgCrabStoredProcFactory;
import org.elasql.migration.mgcrab.MgCrabSystemController;
import org.elasql.migration.planner.MigrationPlanner;
import org.elasql.migration.planner.PredefinedPlanner;
import org.elasql.migration.planner.clay.ClayPlanner;
import org.elasql.migration.squall.SquallMigrationMgr;
import org.elasql.migration.squall.SquallStoredProcFactory;
import org.elasql.migration.stopcopy.StopCopyMigrationMgr;
import org.elasql.migration.stopcopy.StopCopyStoredProcFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public abstract class MigrationComponentFactory {
	private static Logger logger = Logger.getLogger(MigrationComponentFactory.class.getName());
	
	public MigrationComponentFactory() {
		if (logger.isLoggable(Level.INFO))
			logger.info("using " + MigrationSettings.MIGRATION_ALGORITHM + " as migration algorithm.");
	}
	
	public MigrationMgr newMigrationMgr() {
		switch (MigrationSettings.MIGRATION_ALGORITHM) {
		case MGCRAB:
			return new MgCrabMigrationMgr();
		case SQUALL:
			return new SquallMigrationMgr();
		case ALBATROSS:
			return new AlbatrossMigrationMgr();
		case STOP_COPY:
			return new StopCopyMigrationMgr();
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationSystemController newSystemController() {
		switch (MigrationSettings.MIGRATION_ALGORITHM) {
		case MGCRAB:
			return new MgCrabSystemController(this);
		case SQUALL:
			return new MigrationSystemController(this);
		case ALBATROSS:
			return new MigrationSystemController(this);
		case STOP_COPY:
			return new MigrationSystemController(this);
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationStoredProcFactory newMigrationSpFactory(
			CalvinStoredProcedureFactory underlayerFactory) {
		switch (MigrationSettings.MIGRATION_ALGORITHM) {
		case MGCRAB:
			return new MgCrabStoredProcFactory(underlayerFactory);
		case SQUALL:
			return new SquallStoredProcFactory(underlayerFactory);
		case ALBATROSS:
			return new AlbatrossStoredProcFactory(underlayerFactory);
		case STOP_COPY:
			return new StopCopyStoredProcFactory(underlayerFactory);
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationPlanner newMigrationPlanner() {
		switch (MigrationSettings.PLANNING_ALGORITHM) {
		case PREDEFINED:
			return new PredefinedPlanner(newPredefinedMigrationPlan());
		case CLAY:
			return new ClayPlanner();
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public abstract MigrationPlan newPredefinedMigrationPlan();
}
