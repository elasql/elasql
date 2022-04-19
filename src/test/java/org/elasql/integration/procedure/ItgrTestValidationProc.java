package org.elasql.integration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class ItgrTestValidationProc extends TPartStoredProcedure<ItgrTestValidationProcParamHelper> {
	private PrimaryKey[] addKeys = new PrimaryKey[10];
	private int[] valueAns = { 0, 11111, 22222, 33333, 44444, 55555, 66666, 77777, 88888, 99999 };
	private int[] overflowAns = { 0, 14285, 14285, 14285, 14285, 14285, 14285, 14285, 14285, 14285 };

	public ItgrTestValidationProc(long txNum) {
		super(txNum, new ItgrTestValidationProcParamHelper());
	}

	@Override
	public double getWeight() {
		return 10;
	}

	@Override
	protected void prepareKeys() {
		PrimaryKeyBuilder builder;

		// only 10 ids in elasql_test_add table
		for (int i = 0; i < 10; i++) {
			builder = new PrimaryKeyBuilder("elasql_test_add");
			builder.addFldVal("id", new IntegerConstant(i));
			addKeys[i] = builder.build();
			addReadKey(addKeys[i]);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		CachedRecord rec = null;

		for (int i = 0; i < 10; i++) {
			rec = readings.get(addKeys[i]);
			int val = (int) rec.getVal("value").asJavaVal();
			int overflow = (int) rec.getVal("overflow").asJavaVal();

			if (val != valueAns[i] || overflow != overflowAns[i]) {
				throw new RuntimeException(
						String.format("row id: %d, value should be %d not %d, overflow should be %d not %d", i,
								valueAns[i], val, overflowAns[i], overflow));
			}
		}

	}

}
