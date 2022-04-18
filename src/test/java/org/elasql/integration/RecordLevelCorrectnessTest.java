package org.elasql.integration;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;

public class RecordLevelCorrectnessTest {
	private static Logger logger = Logger.getLogger(RecordLevelCorrectnessTest.class.getName());
	
	@BeforeClass
	static void init() {
		ServerInit.init(RecordLevelCorrectnessTest.class);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN PHANTOM TEST");
	}
	
	
	@Test
	public void testRecordCorrectness() {
		ServerInit.init(RecordLevelCorrectnessTest.class);
	}
}
