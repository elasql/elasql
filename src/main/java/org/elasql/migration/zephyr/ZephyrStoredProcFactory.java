package org.elasql.migration.zephyr;

import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class ZephyrStoredProcFactory extends MigrationStoredProcFactory {
	
	public static final int SP_BG_PUSH = -103;
	public static final int SP_DUAL_START = -104;
	
	public ZephyrStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		super(underlayerFactory);
	}
	
	@Override
	protected CalvinStoredProcedure<?> getMigrationStoredProcedure(int pid, long txNum) {
		switch (pid) {
			case SP_BG_PUSH:
				return new BgPushProcedure(txNum);
			case SP_DUAL_START:
				return new ZephyrMigrationChangeProcedure(txNum);
			default:
				return null;
		}
	}
}
