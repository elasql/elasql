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

import org.vanilladb.core.sql.Constant;

public class RecordKeyEntry {
	private String fldName;
	private Constant value;

	public RecordKeyEntry(String fldName, Constant value) {
		this.fldName = fldName;
		this.value = value;
	}

	public String getFldName() {
		return fldName;
	}

	public Constant getValue() {
		return value;
	}

	@Override
	public String toString() {
		return fldName + "=" + value.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		RecordKeyEntry k = (RecordKeyEntry) obj;
		return k.fldName.equals(fldName) && k.value.equals(value);
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + fldName.hashCode();
		hash = hash * 31 + value.hashCode();
		return hash;
	}
}
