package org.elasql.procedure.tpart;

import org.elasql.procedure.DdStoredProcedureFactory;

public interface TPartStoredProcedureFactory
		extends DdStoredProcedureFactory<TPartStoredProcedure<?>> {
	
	@Override
	TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum);

}
