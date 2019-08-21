package org.elasql.migration.mgcrab;

import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class MgCrabStoredProcFactory extends MigrationStoredProcFactory {
	
	public static final int SP_PHASE_CHANGE = -103;
	public static final int SP_ONE_PHASE_BG_PUSH = -104;
	public static final int SP_TWO_PHASE_BG_PUSH = -105;
	
	public MgCrabStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		super(underlayerFactory);
	}
	
	@Override
	protected CalvinStoredProcedure<?> getMigrationStoredProcedure(int pid, long txNum) {
		switch (pid) {
			case SP_PHASE_CHANGE:
				return new PhaseChangeProcedure(txNum);
			case SP_ONE_PHASE_BG_PUSH:
				return new OnePhaseBgPushProcedure(txNum);
			case SP_TWO_PHASE_BG_PUSH:
				return new TwoPhaseBgPushProcedure(txNum);
			default:
				return null;
		}
	}
}
