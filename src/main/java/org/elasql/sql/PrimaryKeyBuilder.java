package org.elasql.sql;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

public class PrimaryKeyBuilder implements Serializable {
	
	private static final long serialVersionUID = 20200206001L;
	
	private String tableName;
	private List<String> fields = new ArrayList<String>();
	private transient List<Constant> values = new ArrayList<Constant>();
	
	public PrimaryKeyBuilder(String tableName) {
		this.tableName = tableName;
	}
	
	public void addFldVal(String field, Constant val) {
		fields.add(field);
		values.add(val);
	}
	
	public void setVal(String field, Constant val) {
		int index = fields.indexOf(field);
		
		if (index == -1)
			throw new FieldNotFoundException(field);
		
		values.set(index, val);
	}
	
	public PrimaryKey build() {
		return new PrimaryKey(tableName,
			fields.toArray(new String[fields.size()]),
			values.toArray(new Constant[fields.size()])
		);
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(values.size());

		// Write out all elements in the proper order
		for (int i = 0; i < values.size(); i++) {
			Constant val = values.get(i);
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
		this.values = new ArrayList<Constant>(numberOfVals);

		// Read in all values
		for (int i = 0; i < numberOfVals; i++) {
			int sqlType = in.readInt();
			int argument = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType, argument), bytes);
			values.add(val);
		}
	}
}
