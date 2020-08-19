package org.elasql.sql;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.index.SearchKey;

public abstract class Key implements Serializable {

	private static final long serialVersionUID = 20200819002L;
	
	private String tableName;
	private String[] fields;
	// We serialize this field manually
	// because a Constant is non-serializable.
	private transient Constant[] values;
	private int hashCode;
	
	protected Key(String tableName, String fld, Constant val) {
		this.tableName = tableName;
		this.fields = new String[] { fld };
		this.values = new Constant[] { val };
		
		genHashCode();
	}
	
	protected Key(String tableName, String[] fields, Constant[] values) {
		if (fields.length != values.length)
			throw new IllegalArgumentException();
		
		this.tableName = tableName;
		this.fields = fields;
		this.values = values;
		
		genHashCode();
	}
	
	protected Key(Key otherKey) {
		this.tableName = otherKey.tableName;
		this.fields = Arrays.copyOf(otherKey.fields, otherKey.fields.length);
		this.values = Arrays.copyOf(otherKey.values, otherKey.values.length);
		
		genHashCode();
	}

	public String getTableName() {
		return tableName;
	}
	
	public boolean containsField(String fld) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].equals(fld))
				return true;
		}
		return false;
	}
	
	public int getNumOfFlds() {
		return fields.length;
	}
	
	public String getField(int index) {
		return fields[index];
	}
	
	public Constant getVal(int index) {
		return values[index];
	}

	public Constant getVal(String fld) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].equals(fld))
				return values[i];
		}
		return null;
	}

	public Predicate toPredicate() {
		Predicate pred = new Predicate();
		for (int i = 0; i < fields.length; i++) {
			Expression k = new FieldNameExpression(fields[i]);
			Expression v = new ConstantExpression(values[i]);
			pred.conjunctWith(new Term(k, Term.OP_EQ, v));
		}
		return pred;
	}
	
	public SearchKey toSearchKey(List<String> indexedFields) {
		Constant[] vals = new Constant[indexedFields.size()];
		Iterator<String> fldNameIter = indexedFields.iterator();

		for (int i = 0; i < vals.length; i++) {
			String fldName = fldNameIter.next();
			vals[i] = getVal(fldName);
			if (vals[i] == null)
				throw new NullPointerException("there is no value for '" + fldName + "'");
		}
		
		return new SearchKey(vals);
	}
	
//	@Override
//	public int compareTo(RecordKey rk) {
//		if (tableName.compareTo("item") == 0) {
//			if ((int)getVal("i_id").asJavaVal() < (int)rk.getVal("i_id").asJavaVal())
//				return -1;
//			else if ((int)getVal("i_id").asJavaVal() > (int)rk.getVal("i_id").asJavaVal())
//				return 1;
//		}
////		else if (tableName.compareTo("ycsb") == 0) {
////			if ((String)getKeyVal("i_id").asJavaVal() < (int)rk.getKeyVal("i_id").asJavaVal())
////				return -1;
////			else if ((int)getKeyVal("i_id").asJavaVal() > (int)rk.getKeyVal("i_id").asJavaVal())
////				return 1;
////		}
//		return 0;
//	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append(tableName);
		sb.append(": ");
		for (int i = 0; i < fields.length; i++) {
			sb.append(fields[i]);
			sb.append(" -> ");
			sb.append(values[i]);
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
		if (obj == null)
			return false;
		if (obj.getClass() != PrimaryKey.class)
			return false;
		Key k = (Key) obj;
		return k.tableName.equals(this.tableName) && Arrays.equals(k.fields, this.fields)
				&& Arrays.equals(k.values, this.values);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	private void genHashCode() {
		hashCode = 17;
		hashCode = 31 * hashCode + tableName.hashCode();
		for (int i = 0; i < fields.length; i++) {
			hashCode = 31 * hashCode + fields[i].hashCode();
			hashCode = 31 * hashCode + values[i].hashCode();
		}
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(values.length);

		// Write out all elements in the proper order
		for (int i = 0; i < values.length; i++) {
			Constant val = values[i];
			byte[] bytes = val.asBytes();
			out.writeInt(val.getType().getSqlType());
			out.writeInt(val.getType().getArgument());
			out.writeInt(bytes.length);
			out.write(bytes);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numberOfVals = in.readInt();
		this.values = new Constant[numberOfVals];

		// Read in all values
		for (int i = 0; i < numberOfVals; i++) {
			int sqlType = in.readInt();
			int argument = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType, argument), bytes);
			values[i] = val;
		}
	}
}
