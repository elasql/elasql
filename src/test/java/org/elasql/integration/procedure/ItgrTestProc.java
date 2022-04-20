package org.elasql.integration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

/*
 * See executeSql for more information about the transaction's logic
 */
public class ItgrTestProc extends TPartStoredProcedure<ItgrTestProcParamHelper> {
	Constant idCon, valueCon, overflowCon;
	
	private PrimaryKey addKey;
	
	public ItgrTestProc(long txNum) {
		super(txNum, new ItgrTestProcParamHelper());
	}

	@Override
	public double getWeight() {
		return 2;
	}

	@Override
	protected void prepareKeys() {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder("elasql_test_add");
		
		idCon = new IntegerConstant((int)(txNum % 10));
		
		builder.addFldVal("id", idCon);
		addKey = builder.build();
		
		addReadKey(addKey);
		addUpdateKey(addKey);
	}

	/*-
	 * SELECT id, value, overflow FROM elasql_test WHERE id = my_tx_id's last digit;
	 * 
	 * Transaction's logic:
	 * ---------------------------------------------------------
	 * new_value = value * 10 + my_tx_id's last digit;
	 * if (new_value > 1_000_000) {
	 * 			new_value = 0;	
	 * 			overflow += 1;
	 * }
	 * ---------------------------------------------------------
	 * 
	 * UPDATE elasql_test SET value=new_value, overflow=overflow where id = my_tx_id's last_digit;
	 */
	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		CachedRecord rec = null;
		
		rec = readings.get(addKey);
		
		int rid = (int) rec.getVal("id").asJavaVal();
		
		int val = (int) rec.getVal("value").asJavaVal();
		val = val * 10 + rid;
		
		int overflow = (int) rec.getVal("overflow").asJavaVal();
		if (val > 1_000_000) {
			val = 0;
			overflow += 1;
			rec.setVal("overflow", new IntegerConstant(overflow));
		}
		rec.setVal("value", new IntegerConstant(val));
		
		update(addKey, rec);
	}
}
