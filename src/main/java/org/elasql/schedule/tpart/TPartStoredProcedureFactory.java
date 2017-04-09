package org.elasql.schedule.tpart;

import org.elasql.schedule.DdStoredProcedureFactory;

public interface TPartStoredProcedureFactory extends DdStoredProcedureFactory{
	
	@Override
	TPartStoredProcedure getStoredProcedure(int pid, long txNum);

}
