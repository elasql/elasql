package org.elasql.migration.sp;

import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class MigrationStoredProcFactory implements CalvinStoredProcedureFactory {
	
	public static final int SP_MIGRATION_START = -101;
	public static final int SP_BG_PUSH = -102;
	public static final int SP_MIGRATION_END = -103;
	
	private CalvinStoredProcedureFactory underlayerFactory;
	
	public MigrationStoredProcFactory(CalvinStoredProcedureFactory underlayerFactory) {
		this.underlayerFactory = underlayerFactory;
	}
	
	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (pid) {
			case SP_MIGRATION_START:
				sp = new MigrationStartProcedure(txNum);
				break;
			case SP_BG_PUSH:
				sp = new BgPushProcedure(txNum);
				break;
			case SP_MIGRATION_END:
				sp = new MigrationEndProcedure(txNum);
				break;
			default:
				sp = underlayerFactory.getStoredProcedure(pid, txNum);
		}
		return sp;
	}
}
