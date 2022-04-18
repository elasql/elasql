package org.elasql.integration;

import java.io.File;

import org.elasql.integration.procedure.ItgrTestStoredProcFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.server.Elasql;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.FileMgr;

public class ServerInit {
	static final String DB_MAIN_DIR = "elasql_testdbs";
	static final int NODE_ID = 0;
	
	static String resetDb(Class<?> testClass) {
		String testClassName = testClass.getName();
		String dbName = DB_MAIN_DIR + "/" + testClassName;
		
		// Creates the main directory if it was not created before
		File dbPath = new File(FileMgr.DB_FILES_DIR, DB_MAIN_DIR);
		if (!dbPath.exists())
			dbPath.mkdir();
		
		// Deletes the existing database
		deleteDB(dbName);
		
		return dbName;
	}
	
	private static void deleteDB(String dbName) {
		File dbPath = new File(FileMgr.DB_FILES_DIR, dbName);
		if (dbPath.exists()) {
			File[] files = dbPath.listFiles();
			for (File file : files) {
				if (!file.delete())
					throw new RuntimeException("cannot delete the file: " + file);
			}
			
			if (!dbPath.delete())
				throw new RuntimeException("cannot delete the directory: " + dbPath);
		}
	}
	
	private static TPartStoredProcedureFactory getTpartSpFactory() {
		return new ItgrTestStoredProcFactory();
	}
	
	static void init(Class<?> testClass) {
		String dbName = resetDb(testClass);
		
		Elasql.init(dbName, NODE_ID, getTpartSpFactory());
	}
}
