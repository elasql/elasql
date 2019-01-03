package org.elasql.migration.albatross;

import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class AlbatrossStoredProcFactory extends MigrationStoredProcFactory {
	
	public static final int SP_BG_PUSH = -103;
	
	public AlbatrossStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		super(underlayerFactory);
	}
	
	@Override
	protected CalvinStoredProcedure<?> getMigrationStoredProcedure(int pid, long txNum) {
		switch (pid) {
			case SP_BG_PUSH:
				return new BgPushProcedure(txNum);
			default:
				return null;
		}
	}
}
