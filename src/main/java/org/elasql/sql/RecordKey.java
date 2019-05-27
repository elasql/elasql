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
package org.elasql.sql;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;

public class RecordKey implements Serializable, Comparable<RecordKey> {

	private static final long serialVersionUID = 20190517001L;
	
	private String tableName;
	private transient String[] fields;
	private transient Constant[] values;
	private int hashCode;
	
	// TODO: Ensure that almost no one use this method
	public RecordKey(String tableName, Map<String, Constant> keyEntryMap) {
		this.tableName = tableName;
		this.fields = new String[keyEntryMap.size()];
		this.values = new Constant[keyEntryMap.size()];
		
		int i = 0;
		for (Map.Entry<String, Constant> entry : keyEntryMap.entrySet()) {
			fields[i] = entry.getKey();
			values[i] = entry.getValue();
			i++;
		}
		
		genHashCode();
	}
	
	public RecordKey(String tableName, String fld, Constant val) {
		this.tableName = tableName;
		this.fields = new String[1];
		this.values = new Constant[1];
		fields[0] = fld;
		values[0] = val;
		genHashCode();
	}

	public RecordKey(String tableName, List<String> flds, List<Constant> vals) {
		this.tableName = tableName;
		if (flds.size() != vals.size())
			throw new IllegalArgumentException();
		
		this.fields = new String[flds.size()];
		this.values = new Constant[flds.size()];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = flds.get(i);
			values[i] = vals.get(i);
		}
		
		genHashCode();
	}

	private void genHashCode() {
		hashCode = 17;
		hashCode = 31 * hashCode + tableName.hashCode();
		for (int i = 0; i < fields.length; i++) {
			hashCode = 31 * hashCode + fields[i].hashCode();
			hashCode = 31 * hashCode + values[i].hashCode();
		}
	}

	public String getTableName() {
		return tableName;
	}
	
	// FIXME: Not immutable
	public String[] getFields() {
		return fields;
	}
	
	public boolean containsField(String fld) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].equals(fld))
				return true;
		}
		return false;
	}

	public Constant getKeyVal(String fld) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].equals(fld))
				return values[i];
		}
		return null;
	}

	public Predicate getPredicate() {
		Predicate pred = new Predicate();
		for (int i = 0; i < fields.length; i++) {
			Expression k = new FieldNameExpression(fields[i]);
			Expression v = new ConstantExpression(values[i]);
			pred.conjunctWith(new Term(k, Term.OP_EQ, v));
		}
		return pred;
	}
	
	@Override
	public int compareTo(RecordKey rk) {
		if (tableName.compareTo("item") == 0) {
			if ((int)getKeyVal("i_id").asJavaVal() < (int)rk.getKeyVal("i_id").asJavaVal())
				return -1;
			else if ((int)getKeyVal("i_id").asJavaVal() > (int)rk.getKeyVal("i_id").asJavaVal())
				return 1;
		}
//		else if (tableName.compareTo("ycsb") == 0) {
//			if ((String)getKeyVal("i_id").asJavaVal() < (int)rk.getKeyVal("i_id").asJavaVal())
//				return -1;
//			else if ((int)getKeyVal("i_id").asJavaVal() > (int)rk.getKeyVal("i_id").asJavaVal())
//				return 1;
//		}
		return 0;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(tableName);
		sb.append(": ");
		for (int i = 0; i < fields.length; i++) {
			sb.append(fields[i]);
			sb.append(" -> ");
			sb.append(values[i]);
			sb.append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != RecordKey.class)
			return false;
		RecordKey k = (RecordKey) obj;
		return k.tableName.equals(this.tableName) && Arrays.equals(k.fields, this.fields)
				&& Arrays.equals(k.values, this.values);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	/**
	 * Serialize this {@code CachedRecord} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(fields.length);

		// Write out all elements in the proper order
		for (int i = 0; i < fields.length; i++) {
			String fld = fields[i];
			Constant val = values[i];
			byte[] bytes = val.asBytes();
			out.writeObject(fld);
			out.writeInt(val.getType().getSqlType());
			out.writeInt(bytes.length);
			out.write(bytes);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numFlds = in.readInt();
		this.fields = new String[numFlds];
		this.values = new Constant[numFlds];

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType), bytes);
			fields[i] = fld;
			values[i] = val;
		}
	}
}
