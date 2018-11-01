package org.elasql.schedule.calvin;

import org.elasql.sql.RecordKey;

public interface ReadWriteSetAnalyzer {
	
	ExecutionPlan generatePlan();

	void addReadKey(RecordKey readKey);

	void addUpdateKey(RecordKey updateKey);
	
	void addInsertKey(RecordKey insertKey);
	
	void addDeleteKey(RecordKey deleteKey);
	
}
