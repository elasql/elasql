package org.elasql.sql;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.Constant;

public class RecordKeyBuilder {
	
	private String tableName;
	private List<String> fields = new ArrayList<String>();
	private List<Constant> values = new ArrayList<Constant>();
	
	public RecordKeyBuilder(String tableName) {
		this.tableName = tableName;
	}
	
	public void addFldVal(String field, Constant val) {
		fields.add(field);
		values.add(val);
	}
	
	public RecordKey build() {
		return new RecordKey(tableName,
			fields.toArray(new String[fields.size()]),
			values.toArray(new Constant[fields.size()])
		);
	}
}
