package org.elasql.integration.procedure;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.integration.IntegrationTest;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class ItgrTestValidationProc extends TPartStoredProcedure<ItgrTestValidationProcParamHelper> {
	private static Logger logger = Logger.getLogger(IntegrationTest.class.getName());

	private PrimaryKey[] addKeys = new PrimaryKey[10];

	private int[] valueAns;
	private int[] overflowAns;

	public ItgrTestValidationProc(long txNum) {
		super(txNum, new ItgrTestValidationProcParamHelper());

		if (IntegrationTest.TX_NUMS == 10000) {
			int[] values = { 0, 111111, 222222, 333333, 444444, 555555, 666666, 777777, 888888, 999999 };
			int[] overflows = { 0, 142, 142, 142, 142, 142, 142, 142, 142, 142 };
			valueAns = values;
			overflowAns = overflows;
		} else if (IntegrationTest.TX_NUMS == 1000000) {
			int[] values = { 0, 11111, 22222, 33333, 44444, 55555, 66666, 77777, 88888, 99999 };
			int[] overflows = { 0, 14285, 14285, 14285, 14285, 14285, 14285, 14285, 14285, 14285 };
			valueAns = values;
			overflowAns = overflows;
		} else {
			throw new RuntimeException("TX_NUMS should be 10000 or 1000000, otherwise you should add a new answer.");
		}
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
						String.format("row id: %d, value should be %d and the real value is %d, overflow should be %d and the real overflow is %d", i,
								valueAns[i], val, overflowAns[i], overflow));
			} else {
				if (logger.isLoggable(Level.INFO)) {
					logger.info(String.format("row id %d is correct", i));
				}
			}
		}

	}

}
