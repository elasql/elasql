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

public class RecordVersion {
	public RecordKey key;
	public long srcTxNum;

	public RecordVersion(RecordKey key, long srcTxNum) {
		this.key = key;
		this.srcTxNum = srcTxNum;
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + key.hashCode();
		hashCode = 31 * hashCode
				+ (int) (this.srcTxNum ^ (this.srcTxNum >>> 32));
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != RecordVersion.class)
			return false;
		RecordVersion rv = (RecordVersion) obj;
		return rv.key.equals(this.key) && rv.srcTxNum == this.srcTxNum;
	}

	@Override
	public String toString() {
		return srcTxNum + "#" + key.toString();
	}
}
