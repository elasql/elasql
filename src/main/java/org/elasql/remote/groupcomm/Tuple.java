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
package org.elasql.remote.groupcomm;

import java.io.Serializable;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.PrimaryKey;

public class Tuple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -606284893049245719L;
	public PrimaryKey key;
	public CachedRecord rec;
	public long srcTxNum;
	public long destTxNum;

	public Tuple(PrimaryKey key, long srcTxNum, long destTxNum, CachedRecord rec) {
		this.key = key;
		this.rec = rec;
		this.srcTxNum = srcTxNum;
		this.destTxNum = destTxNum;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("[Tuple: ");
		sb.append(key);
		sb.append(" sent form tx.");
		sb.append(srcTxNum);
		sb.append(" to tx.");
		sb.append(destTxNum);
		sb.append("]");

		return sb.toString();
	}

}
