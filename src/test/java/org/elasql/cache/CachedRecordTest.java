package org.elasql.cache;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.junit.Test;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class CachedRecordTest {
	
	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		// Build a key
		PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("test_table");
		keyBuilder.addFldVal("test_key_int", new IntegerConstant(1));
		keyBuilder.addFldVal("test_key_str", new VarcharConstant("test_val"));
		PrimaryKey key = keyBuilder.build();
		
		// Build a cached record
		CachedRecord rec = new CachedRecord(key);
		rec.addFldVal("test_field_int", new IntegerConstant(2));
		rec.addFldVal("test_field_str", new VarcharConstant("test_val2"));
		
		CachedRecord result = null;
		byte[] bytes = null;
		
		// Serialize the object to a byte array
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
				out.writeObject(rec);
				out.flush();
				bytes = bos.toByteArray();
			}
		}
		
		// Deserialize the byte array
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
			try (ObjectInputStream in = new ObjectInputStream(bis)) {
				result = (CachedRecord) in.readObject();
			}
		}
		
		assertEquals("fails to deserialize the object of CachedRecord", rec, result);
	}
}
