package org.elasql.server.migration.procedure;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.remote.groupcomm.server.ConnectionMgr;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MigrationAnalysisProc extends CalvinStoredProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(MigrationAnalysisProc.class.getName());
	
	private static final int WAITITING_TIME = 0; // in ms
	
	private int sourceNode = Elasql.migrationMgr().getSourcePartition();
	
	public MigrationAnalysisProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		//Gobal set Analysis flags
		Elasql.migrationMgr().startAnalysis();
	}

	@Override
	public void prepareKeys() {
		Elasql.migrationMgr().prepareAnalysis();
	}
	
	@Override
	protected void executeTransactionLogic() {
		
		// Only Migration SourceNode perform Analysis process
		if (Elasql.serverId() == sourceNode) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Start analysis phase for migration...");
			
			try {
				Thread.sleep(WAITITING_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Waiting completed.");

			// Generate data set
			Map<RecordKey, Boolean> dataSet = Elasql.migrationMgr().generateDataSetForMigration();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Estimated size of target data set for migration: "
						+ dataSet.size());
			
			// Send the target data set to the migration manager
			Elasql.migrationMgr().analysisComplete(dataSet);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Finish analysis.");
			
			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_START_MIGRATION);
			Elasql.connectionMgr().pushTupleSet(ConnectionMgr.SEQ_NODE_ID, ts);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Sent the migration starting request.");
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		//Do nothing
		
	}
}
