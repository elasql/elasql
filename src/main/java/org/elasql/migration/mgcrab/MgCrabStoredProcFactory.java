package org.elasql.migration.mgcrab;

import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class MgCrabStoredProcFactory extends MigrationStoredProcFactory {
	
	public static final int SP_BG_PUSH = -103;
	public static final int SP_PHASE_CHANGE = -104;
	
	public MgCrabStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		super(underlayerFactory);
	}
	
	@Override
	protected CalvinStoredProcedure<?> getMigrationStoredProcedure(int pid, long txNum) {
		switch (pid) {
			case SP_BG_PUSH:
				return new BgPushProcedure(txNum);
			case SP_PHASE_CHANGE:
				return new PhaseChangeProcedure(txNum);
			default:
				return null;
		}
	}
}
