package org.elasql.migration;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.albatross.AlbatrossMigrationMgr;
import org.elasql.migration.albatross.AlbatrossStoredProcFactory;
import org.elasql.migration.albatross.AlbatrossSystemController;
import org.elasql.migration.mgcrab.MgCrabMigrationMgr;
import org.elasql.migration.mgcrab.MgCrabStoredProcFactory;
import org.elasql.migration.mgcrab.MgCrabSystemController;
import org.elasql.migration.squall.SquallMigrationMgr;
import org.elasql.migration.squall.SquallStoredProcFactory;
import org.elasql.migration.squall.SquallSystemController;
import org.elasql.migration.stopcopy.StopCopyMigrationMgr;
import org.elasql.migration.stopcopy.StopCopyStoredProcFactory;
import org.elasql.migration.stopcopy.StopCopySystemController;
import org.elasql.migration.zephyr.ZephyrMigrationMgr;
import org.elasql.migration.zephyr.ZephyrStoredProcFactory;
import org.elasql.migration.zephyr.ZephyrSystemController;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.storage.metadata.PartitionPlan;

public abstract class MigrationComponentFactory {
	private static Logger logger = Logger.getLogger(MigrationComponentFactory.class.getName());
	
	public static final MigrationAlgorithm CURRENT_ALGO = MigrationAlgorithm.Zephyr;
	
	public MigrationComponentFactory() {
		if (logger.isLoggable(Level.INFO))
			logger.info("using " + CURRENT_ALGO + " as migration algorithm.");
	}
	
	public MigrationMgr newMigrationMgr() {
		switch (CURRENT_ALGO) {
		case MGCRAB:
			return new MgCrabMigrationMgr(this);
		case SQUALL:
			return new SquallMigrationMgr(this);
		case ALBATROSS:
			return new AlbatrossMigrationMgr(this);
		case STOP_COPY:
			return new StopCopyMigrationMgr(this);
		case Zephyr:
			return new ZephyrMigrationMgr(this);
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationSystemController newSystemController() {
		switch (CURRENT_ALGO) {
		case MGCRAB:
			return new MgCrabSystemController(this);
		case SQUALL:
			return new SquallSystemController(this);
		case ALBATROSS:
			return new AlbatrossSystemController(this);
		case STOP_COPY:
			return new StopCopySystemController(this);
		case Zephyr:
			return new ZephyrSystemController(this);
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public MigrationStoredProcFactory newMigrationSpFactory(
			CalvinStoredProcedureFactory underlayerFactory) {
		switch (CURRENT_ALGO) {
		case MGCRAB:
			return new MgCrabStoredProcFactory(underlayerFactory);
		case SQUALL:
			return new SquallStoredProcFactory(underlayerFactory);
		case ALBATROSS:
			return new AlbatrossStoredProcFactory(underlayerFactory);
		case STOP_COPY:
			return new StopCopyStoredProcFactory(underlayerFactory);
		case Zephyr:
			return new ZephyrStoredProcFactory(underlayerFactory);
		}
		throw new RuntimeException("it should not be here.");
	}
	
	public abstract List<MigrationRange> generateMigrationRanges(PartitionPlan oldPlan, PartitionPlan newPlan);
	
	public abstract PartitionPlan newPartitionPlan();
}
