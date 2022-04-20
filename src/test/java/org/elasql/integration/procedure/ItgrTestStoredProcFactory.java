package org.elasql.integration.procedure;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;

public class ItgrTestStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp = null;

		switch (pid) {
		case 0:
			sp = new ItgrTestProc(txNum);
			break;
		case 1:
			sp = new ItgrTestValidationProc(txNum);
		default:
			break;
		}
		return sp;
	}
}
