/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.sql;

import org.vanilladb.core.sql.Constant;

/**
 * A {@code PrimaryKey} object is an immutable object that represents a primary key.
 * 
 * @author SLMT
 *
 */
public class PrimaryKey extends Key {

	private static final long serialVersionUID = 20200819003L;
	
	public PrimaryKey(String tableName, String fld, Constant val) {
		super(tableName, fld, val);
	}
	
	/**
	 * Constructs a RecordKey with the given field array and value array.
	 * This method should be only called by {@code PrimaryKeyBuilder}.
	 * 
	 * @param tableName
	 * @param fields
	 * @param values
	 */
	PrimaryKey(String tableName, String[] fields, Constant[] values) {
		super(tableName, fields, values);
	}
}
