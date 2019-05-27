/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

public class CachedRecord implements Record, Serializable {

	private static final long serialVersionUID = 20190517001L;

	private boolean isDirty, isDeleted, isNewInserted;
	private long srcTxNum;
	
	private RecordKey primaryKey;
	private transient ArrayList<String> fields = new ArrayList<String>();
	private transient ArrayList<Constant> values = new ArrayList<Constant>();
	private transient ArrayList<String> dirtyFlds = new ArrayList<String>();

	public CachedRecord(RecordKey primaryKey) {
		this.primaryKey = primaryKey;
	}

	public CachedRecord(RecordKey primaryKey, Map<String, Constant> fldVals) {
		for (Map.Entry<String, Constant> entry : fldVals.entrySet()) {
			if (!primaryKey.containsField(entry.getKey())) {
				fields.add(entry.getKey());
				values.add(entry.getValue());
			}
		}
	}
	
	/**
	 * Constructs a new CachedRecord with the same key-value pairs
	 * and the same meta-data as the given CachedRecord.
	 */
	public CachedRecord(CachedRecord rec) {
		primaryKey = rec.primaryKey;
		fields = new ArrayList<String>(rec.fields);
		values = new ArrayList<Constant>(rec.values);
		dirtyFlds = new ArrayList<String>(rec.dirtyFlds);
		isDirty = rec.isDirty;
		isDeleted = rec.isDeleted;
		isNewInserted = rec.isNewInserted;
		srcTxNum = rec.srcTxNum;
	}

	public Constant getVal(String fldName) {
		if (isDeleted) {
			return null;
		} else {
			// Check the key first
			Constant val = primaryKey.getKeyVal(fldName);
			if (val != null)
				return val;
			
			// Check the array
			int index = fields.indexOf(fldName);
			if (index != -1)
				return values.get(index);
			return null;
		}
	}

	public boolean setVal(String fldName, Constant val) {
		if (isDeleted)
			return false;
		
		if (primaryKey.containsField(fldName))
			return false;
		
		isDirty = true;
		if (!dirtyFlds.contains(fldName))
			dirtyFlds.add(fldName);
		
		int index = fields.indexOf(fldName);
		if (index == -1) {
			fields.add(fldName);
			values.add(val);
		} else {
			values.set(index, val);
		}
		return true;
	}

	public void delete() {
		isDeleted = true;
		isDirty = true;
	}

	public void setNewInserted(boolean isNewInserted) {
		this.isNewInserted = isNewInserted;
	}

	public boolean isDirty() {
		return isDirty;
	}

	public boolean isDeleted() {
		return isDeleted;
	}

	public boolean isNewInserted() {
		return isNewInserted;
	}
	
	public List<String> getFldNames() {
		return fields;
	}
	
	public List<String> getDirtyFldNames() {
		return dirtyFlds;
	}

	public long getSrcTxNum() {
		return srcTxNum;
	}

	public void setSrcTxNum(long srcTxNum) {
		this.srcTxNum = srcTxNum;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(primaryKey.getTableName());
		sb.append(": {");
		
		// Key fields
		for (String field : primaryKey.getFields()) {
			sb.append("*");
			sb.append(field);
			sb.append(": ");
			sb.append(primaryKey.getKeyVal(field));
			sb.append(", ");
		}
		
		// Other fields
		for (int i = 0; i < fields.size(); i++) {
			sb.append(fields.get(i));
			sb.append(": ");
			sb.append(values.get(i));
			sb.append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append("}");
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof CachedRecord))
			return false;
		CachedRecord rec = (CachedRecord) obj;
		return rec.primaryKey.equals(this.primaryKey) &&
				rec.fields.equals(this.fields) &&
				rec.values.equals(this.values) &&
				rec.srcTxNum == this.srcTxNum;
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + primaryKey.hashCode();
		for (int i = 0; i < fields.size(); i++) {
			hashCode = 31 * hashCode + fields.get(i).hashCode();
			hashCode = 31 * hashCode + values.get(i).hashCode();
		}
		hashCode = 31 * hashCode + (int) (srcTxNum ^ (srcTxNum >>> 32));
		return hashCode;
	}

	/**
	 * Serialize this {@code CachedRecord} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(fields.size());

		// Write out all elements in the proper order
		for (int i = 0; i < fields.size(); i++) {
			String fld = fields.get(i);
			Constant val = values.get(i);
			byte[] bytes = val.asBytes();
			out.writeObject(fld);
			out.writeInt(val.getType().getSqlType());
			out.writeInt(bytes.length);
			out.write(bytes);
		}
		out.writeInt(dirtyFlds.size());
		for (String fld : dirtyFlds) {
			out.writeObject(fld);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		int numFlds = in.readInt();
		this.fields = new ArrayList<String>(numFlds);
		this.values = new ArrayList<Constant>(numFlds);

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType),
					bytes);
			fields.add(fld);
			values.add(val);
		}

		dirtyFlds = new ArrayList<String>();
		int dirtyNum = in.readInt();
		for (int i = 0; i < dirtyNum; ++i) {
			dirtyFlds.add((String) in.readObject());
		}
	}
}
