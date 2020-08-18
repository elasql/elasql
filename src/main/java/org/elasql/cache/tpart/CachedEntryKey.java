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
package org.elasql.cache.tpart;

import org.elasql.sql.PrimaryKey;

public class CachedEntryKey {
	private final PrimaryKey recKey;
	private final long source;
	private final long dest;

	public CachedEntryKey(PrimaryKey key, long src, long dest) {
		recKey = key;
		source = src;
		this.dest = dest;
	}

	public PrimaryKey getRecordKey() {
		return recKey;
	}

	public long getSource() {
		return source;
	}

	public long getDestination() {
		return dest;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != CachedEntryKey.class)
			return false;
		CachedEntryKey key = (CachedEntryKey) obj;
		return key.recKey.equals(recKey) && key.source == this.source && key.dest == this.dest;
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + recKey.hashCode();
		hashCode = 31 * hashCode + (int) (dest ^ (dest >>> 32));
		hashCode = 31 * hashCode + (int) (source ^ (source >>> 32));
		return hashCode;
	}

	@Override
	public String toString() {
		return "[" + recKey.toString() + ", src:" + source + ", dest:" + dest + "]";
	}
}
