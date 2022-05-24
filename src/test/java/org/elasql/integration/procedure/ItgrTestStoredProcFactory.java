package org.elasql.integration.procedure;

import org.elasql.integration.IntegrationTest;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;

public class ItgrTestStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp = null;

		switch (pid) {
		case IntegrationTest.ITGR_TEST_PROC_ID:
			sp = new ItgrTestProc(txNum);
			break;
		case IntegrationTest.ITGR_TEST_VALIDATION_PROC_ID:
			sp = new ItgrTestValidationProc(txNum);
			break;
		default:
			break;
		}
		return sp;
	}
}
