package org.elasql.schedule.calvin;

import org.elasql.sql.PrimaryKey;

public interface ReadWriteSetAnalyzer {
	
	ExecutionPlan generatePlan();

	void addReadKey(PrimaryKey readKey);

	void addUpdateKey(PrimaryKey updateKey);
	
	void addInsertKey(PrimaryKey insertKey);
	
	void addDeleteKey(PrimaryKey deleteKey);
	
}
