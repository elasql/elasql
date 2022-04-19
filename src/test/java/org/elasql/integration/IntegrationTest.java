package org.elasql.integration;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class IntegrationTest {
	private static Logger logger = Logger.getLogger(IntegrationTest.class.getName());
	
	@BeforeClass
	public static void init() {
		ServerInit.init(IntegrationTest.class);
		ServerInit.loadTestBed();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Begin integration test");
	}
	
	
	@Test
	public void testRecordCorrectness() {
		Assert.assertTrue(false);
	}
}
