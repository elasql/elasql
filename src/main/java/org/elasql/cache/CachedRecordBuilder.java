package org.elasql.cache;

import java.util.ArrayList;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

public class CachedRecordBuilder {
	
	private RecordKey key;
	private ArrayList<String> fields = new ArrayList<String>();
	private ArrayList<Constant> values = new ArrayList<Constant>();
	
	public CachedRecordBuilder(RecordKey key) {
		this.key = key;
	}
	
	public CachedRecordBuilder addField(String fldName, Constant val) {
		// Note: Should we check if the field has existed here?
		fields.add(fldName);
		values.add(val);
		
		return this;
	}
	
	public CachedRecord build() {
		return new CachedRecord(key, fields, values);
	}
}
