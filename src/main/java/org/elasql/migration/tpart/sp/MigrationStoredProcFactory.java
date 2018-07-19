package org.elasql.migration.tpart.sp;

import org.elasql.migration.MigrationMgr;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;

public class MigrationStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (pid) {
			case MigrationMgr.SP_MIGRATION_START:
				sp = new MigrationStartProcedure(txNum);
				break;
			case MigrationMgr.SP_COLD_MIGRATION:
				sp = new ColdMigrationProcedure(txNum);
				break;
			case MigrationMgr.SP_MIGRATION_END:
				sp = new MigrationEndProcedure(txNum);
				break;
			default:
				throw new UnsupportedOperationException("Procedure " + pid + " is not found");
		}
		return sp;
	}
}
