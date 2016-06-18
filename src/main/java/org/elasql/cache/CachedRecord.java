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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

public class CachedRecord implements Record, Serializable {

	private static final long serialVersionUID = 245365697121L;

	private transient Map<String, Constant> fldValueMap;

	private boolean isDirty, isDeleted, isNewInserted;
	private long srcTxNum;
	private transient Set<String> dirtyFlds = new HashSet<String>();

	public CachedRecord() {
		fldValueMap = new HashMap<String, Constant>();
	}

	public CachedRecord(Map<String, Constant> fldVals) {
		fldValueMap = fldVals;
	}

	public Constant getVal(String fldName) {
		return isDeleted ? null : fldValueMap.get(fldName);
	}

	public boolean setVal(String fldName, Constant val) {
		if (isDeleted)
			return false;
		isDirty = true;
		dirtyFlds.add(fldName);
		fldValueMap.put(fldName, val);
		return true;
	}

	public boolean setVals(Map<String, Constant> fldVals) {
		if (isDeleted)
			return false;
		isDirty = true;

		dirtyFlds.addAll(fldVals.keySet());
		fldValueMap.putAll(fldVals);
		return true;
	}

	public void delete() {
		isDeleted = true;
		isDirty = true;
	}

	public void setNewInserted(boolean isNewInserted) {
		this.isNewInserted = isNewInserted;
	}

	public void setDirty(boolean isDirty) {
		this.isDirty = isDirty;
	}

	public void setDeleted(boolean isDeleted) {
		this.isDeleted = isDeleted;
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

	public Set<String> getFldNames() {
		return fldValueMap.keySet();
	}

	public Map<String, Constant> getFldValMap() {
		return fldValueMap;
	}

	public Set<String> getDirtyFldNames() {
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
		StringBuilder sb = new StringBuilder("[");
		Set<String> flds = new TreeSet<String>(fldValueMap.keySet());
		for (String fld : flds)
			sb.append(fld).append("=").append(fldValueMap.get(fld))
					.append(", ");
		if (flds.size() > 0) {
			int end = sb.length();
			sb.replace(end - 2, end, "] ");
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof CachedRecord))
			return false;
		CachedRecord s = (CachedRecord) obj;
		return s.fldValueMap.equals(this.fldValueMap)
				&& s.srcTxNum == this.srcTxNum;
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + fldValueMap.hashCode();
		hashCode = 31 * hashCode + (int) (srcTxNum ^ (srcTxNum >>> 32));
		return hashCode;
	}

	/**
	 * Serialize this {@code CachedRecord} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		Set<String> fldsSet = fldValueMap.keySet();
		out.defaultWriteObject();
		out.writeInt(fldsSet.size());

		// Write out all elements in the proper order
		for (String fld : fldsSet) {
			Constant val = fldValueMap.get(fld);
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
		fldValueMap = new HashMap<String, Constant>();
		int numFlds = in.readInt();

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType),
					bytes);
			fldValueMap.put(fld, val);
		}

		dirtyFlds = new HashSet<String>();
		int dirtyNum = in.readInt();
		for (int i = 0; i < dirtyNum; ++i) {
			dirtyFlds.add((String) in.readObject());
		}
	}
}
