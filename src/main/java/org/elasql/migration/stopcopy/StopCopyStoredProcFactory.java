package org.elasql.migration.stopcopy;

import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class StopCopyStoredProcFactory extends MigrationStoredProcFactory {
	
	public static final int SP_BG_PUSH = -103;
	
	public StopCopyStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		super(underlayerFactory);
	}
	
	@Override
	protected CalvinStoredProcedure<?> getMigrationStoredProcedure(int pid, long txNum) {
		switch (pid) {
			default:
				return null;
		}
	}
}
