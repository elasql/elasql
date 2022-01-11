package org.elasql.perf.tpart.workload;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RecordSizeMaintainer {
	
	// Record size for each table
	private static final Map<String, Integer> RECORD_SIZES;	
	
	static {
		Map<String, Integer> recordSizes = new HashMap<String, Integer>();
		
		recordSizes.put("ycsb", 1022);
		
		recordSizes.put("warehouse", 765);
		recordSizes.put("district", 817);
		recordSizes.put("customer", 1551);
		recordSizes.put("item", 632);
		recordSizes.put("stock", 1197);
		recordSizes.put("order_line", 795);
		recordSizes.put("orders", 693);
		recordSizes.put("history", 725);
		recordSizes.put("new_order", 546);
		
		RECORD_SIZES = Collections.unmodifiableMap(recordSizes);
	}
	
	public static int getRecordSize(String tableName) {
		Integer tableSize = RECORD_SIZES.get(tableName);
		if(tableSize == null) {
			throw new RuntimeException("Can't find the record size for the table."); 
		}
		return tableSize;
	}
}
