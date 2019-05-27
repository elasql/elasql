package org.elasql.storage.metadata;

import java.util.HashMap;
import java.util.Map;

import org.elasql.sql.RecordKey;
import org.junit.Test;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

import junit.framework.Assert;

public class FusionTableTest {

	private static final String TEST_TABLE = "test-table";
	private static final String TEST_FIELD = "test-field";
	
	@Test
	public void testSetLocation() {
		FusionTable ft = new FusionTable(10);
		RecordKey[] keys = new RecordKey[10];
		
		for (int i = 0; i < 10; i++) {
			keys[i] = new RecordKey(TEST_TABLE, TEST_FIELD, new IntegerConstant(i));
			ft.setLocation(keys[i], i);
		}
		
		for (int i = 0; i < 10; i++) {
			Assert.assertEquals(i, ft.getLocation(keys[i]));
		}
	}
	
	@Test
	public void testRepeatedSetLocation() {
		FusionTable ft = new FusionTable(10);
		RecordKey[] keys = new RecordKey[10];
		
		for (int i = 0; i < 10; i++) {
			keys[i] = new RecordKey(TEST_TABLE, TEST_FIELD, new IntegerConstant(i));
			ft.setLocation(keys[i], i);
		}
		
		for (int i = 0; i < 5; i++)
			ft.setLocation(keys[0], i);
		
		for (int i = 1; i < 10; i++) {
			Assert.assertEquals(i, ft.getLocation(keys[i]));
		}
		Assert.assertEquals(4, ft.getLocation(keys[0]));
	}
	
	@Test
	public void testRemove() {
		FusionTable ft = new FusionTable(10);
		RecordKey[] keys = new RecordKey[10];
		
		for (int i = 0; i < 10; i++) {
			keys[i] = new RecordKey(TEST_TABLE, TEST_FIELD, new IntegerConstant(i));
			ft.setLocation(keys[i], i);
		}
		
		for (int i = 0; i < 10; i += 2)
			ft.remove(keys[i]);
		
		for (int i = 0; i < 10; i++) {
			if (i % 2 == 0)
				Assert.assertEquals(-1, ft.getLocation(keys[i]));
			else
				Assert.assertEquals(i, ft.getLocation(keys[i]));
		}
	}
	
	@Test
	public void testRemoveThenSet() {
		FusionTable ft = new FusionTable(15);
		RecordKey[] keys = new RecordKey[15];
		
		for (int i = 0; i < 10; i++) {
			keys[i] = new RecordKey(TEST_TABLE, TEST_FIELD, new IntegerConstant(i));
			ft.setLocation(keys[i], i);
		}
		
		for (int i = 0; i < 10; i += 2)
			ft.remove(keys[i]);
		
		for (int i = 10; i < 15; i++) {
			keys[i] = new RecordKey(TEST_TABLE, TEST_FIELD, new IntegerConstant(i));
			ft.setLocation(keys[i], i);
		}
		
		for (int i = 0; i < 10; i++) {
			if (i % 2 == 0)
				Assert.assertEquals(-1, ft.getLocation(keys[i]));
			else
				Assert.assertEquals(i, ft.getLocation(keys[i]));
		}
		
		for (int i = 10; i < 15; i++) {
			Assert.assertEquals(i, ft.getLocation(keys[i]));
		}
	}
	
	@Test
	public void testOverflow() {
		FusionTable ft = new FusionTable(10);
		RecordKey[] keys = new RecordKey[20];
		
		for (int i = 0; i < 20; i++) {
			keys[i] = new RecordKey(TEST_TABLE, TEST_FIELD, new IntegerConstant(i));
			ft.setLocation(keys[i], i);
		}
		
		for (int i = 0; i < 20; i++) {
			Assert.assertEquals(i, ft.getLocation(keys[i]));
		}
		
		Assert.assertEquals(20, ft.size());
		
		Assert.assertEquals(10, ft.getOverflowKeys().size());
		for (RecordKey key : ft.getOverflowKeys()) {
			Assert.assertTrue(ft.remove(key) != -1);
		}
		
		Assert.assertEquals(10, ft.size());
	}
}
