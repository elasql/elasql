package org.elasql.integration.procedure;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ItgrTestProcParamHelper extends StoredProcedureParamHelper {

	protected int id, value, overflow;
	
	
	@Override
	public void prepareParameters(Object... pars) {
		// ignore
	}

	@Override
	public Schema getResultSetSchema() {
		Schema sch = new Schema();
		sch.addField("id", Type.INTEGER);
		sch.addField("value", Type.INTEGER);
		sch.addField("overflow", Type.INTEGER);
		
		return sch;
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		SpResultRecord rec = new SpResultRecord();
		
		rec.setVal("id", new IntegerConstant(id));
		rec.setVal("value", new IntegerConstant(id));
		rec.setVal("overflow", new IntegerConstant(id));
		
		return rec;
	}
	
	IntegerConstant getId() {
		return new IntegerConstant(id);
	}
	
	IntegerConstant getValue() {
		return new IntegerConstant(value);
	}
	
	IntegerConstant getOverflow() {
		return new IntegerConstant(overflow);
	}
}
