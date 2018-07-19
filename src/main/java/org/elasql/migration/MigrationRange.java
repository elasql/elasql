package org.elasql.migration;

import java.io.Serializable;

public class MigrationRange implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String table;
	private String keyField;
	private int startId;
	private int endId;
	
	private int size;
	
	private int srcPartId;
	private int destPartId;
	
	// both startId and endId are inclusive
	public MigrationRange(String table, String keyField, int startId, int endId,
			int sourcePartId, int destPartId) {
		this.table = table;
		this.keyField = keyField;
		this.startId = startId;
		this.endId = endId;
		this.size = endId - startId + 1;
		this.srcPartId = sourcePartId;
		this.destPartId = destPartId;
	}
	
	public MigrationRange cutASlice(int cutSize) {
		// No more for cutting
		if (cutSize >= size)
			return null;
		
		int sliceStart = startId;
		int sliceEnd = startId + cutSize - 1;
		startId = sliceEnd + 1;
		size = endId - startId + 1;
		
		return new MigrationRange(table, keyField, sliceStart,
				sliceEnd, srcPartId, destPartId);
	}
	
//	public boolean isOverlapping(MigrationRange range) {
//		return table.equals(range.table) && keyField.equals(range.keyField) &&
//				startId <= range.endId && range.startId <= endId; 
//	}
//	
//	public boolean cutOff(MigrationRange range) {
//		if (!isOverlapping(range))
//			return false;
//		
//		if (startId > range.startId)
//			startId = Math.max(startId, range.endId);
//		if (endId < range.endId)
//			endId = Math.min(endId, range.startId);
//		
//		return true;
//	}
	
	public boolean contains(int id) {
		return startId <= id && id <= endId;
	}
	
	public String getTableName() {
		return table;
	}
	
	public String getKeyFieldName() {
		return keyField;
	}
	
	public int getStartId() {
		return startId;
	}
	
	public int getEndId() {
		return endId;
	}
	
	public int getSourcePartId() {
		return srcPartId;
	}
	
	public int getDestPartId() {
		return destPartId;
	}
	
	@Override
	public String toString() {
		return String.format("[%s %s: %d ~ %d, part.%d -> part.%d, %d records]", table, keyField,
				startId, endId, srcPartId, destPartId, size);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!obj.getClass().equals(this.getClass()))
			return false;
		
		MigrationRange range = (MigrationRange) obj;
		
		return table.equals(range.table) && keyField.equals(range.keyField) &&
				startId == range.startId && endId == range.endId &&
				srcPartId == range.srcPartId && destPartId == range.destPartId;
	}
}
