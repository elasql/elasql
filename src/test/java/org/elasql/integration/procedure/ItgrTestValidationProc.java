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
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ItgrTestValidationProc extends TPartStoredProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(IntegrationTest.class.getName());

	private PrimaryKey[] addKeys = new PrimaryKey[10];

	public ItgrTestValidationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.newDefaultParamHelper());
	}

	@Override
	public double getWeight() {
		return 10;
	}

	@Override
	protected void prepareKeys() {
		PrimaryKeyBuilder builder;

		// only IntegrationTest.TABLE_ROW_NUM ids in elasql_test_add table
		for (int i = 0; i < IntegrationTest.TABLE_ROW_NUM; i++) {
			builder = new PrimaryKeyBuilder("elasql_test_add");
			builder.addFldVal("id", new IntegerConstant(i));
			addKeys[i] = builder.build();
			addReadKey(addKeys[i]);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		CachedRecord rec = null;

		int[] valueAns = new int[10];
		int[] overflowAns = new int[10];

		calculateCurrentAnswer(valueAns, overflowAns);

		for (int i = 0; i < IntegrationTest.TABLE_ROW_NUM; i++) {
			rec = readings.get(addKeys[i]);
			int val = (int) rec.getVal("value").asJavaVal();
			int overflow = (int) rec.getVal("overflow").asJavaVal();

			if (val != valueAns[i] || overflow != overflowAns[i]) {
				throw new RuntimeException(String.format(
						"row id: %d, value should be %d and the real value is %d, overflow should be %d and the real overflow is %d",
						i, valueAns[i], val, overflowAns[i], overflow));
			} else {
				if (logger.isLoggable(Level.INFO)) {
					logger.info(String.format("row id %d is correct", i));
				}
			}
		}
	}

	void calculateCurrentAnswer(int[] valueAns, int[] overflowAns) {
		int[][] fakeTbl = new int[10][3];

		// initialize fakeTbl
		for (int rid = 0; rid < IntegrationTest.TABLE_ROW_NUM; rid++) {
			fakeTbl[rid][0] = rid;
			fakeTbl[rid][1] = 0;
			fakeTbl[rid][2] = 0;
		}

		// deterministic calculate fakeTbl results
		for (int txId = 0; txId < txNum; txId++) {
			int rid = fakeTbl[txId % 10][0];

			int value = fakeTbl[rid][1];
			int overflow = fakeTbl[rid][2];

			value = value * 10 + rid;
			if (value > 1_000_000) {
				value = 0;
				overflow += 1;
			}

			fakeTbl[rid][1] = value;
			fakeTbl[rid][2] = overflow;
		}

		// apply answer back
		for (int rid = 0; rid < IntegrationTest.TABLE_ROW_NUM; rid++) {
			valueAns[rid] = fakeTbl[rid][1];
			overflowAns[rid] = fakeTbl[rid][2];
		}
	}
}
