package org.elasql.perf.tpart.control;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;

public class ControlStoredProcedureFactory implements TPartStoredProcedureFactory {

	public static final int SP_CONTROL_PARAM_UPDATE = -20210921;

	private TPartStoredProcedureFactory underlayerFactory;
	
	public ControlStoredProcedureFactory(TPartStoredProcedureFactory underlayerFactory) {
		this.underlayerFactory = underlayerFactory;
	}
	
	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (pid) {
			case SP_CONTROL_PARAM_UPDATE:
				sp = new ControlParamUpdateProcedure(txNum);
				break;
			default:
				sp = underlayerFactory.getStoredProcedure(pid, txNum);
		}
		return sp;
	}
}
