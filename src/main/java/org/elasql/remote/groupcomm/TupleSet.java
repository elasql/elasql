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
import java.util.ArrayList;
import java.util.List;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.PrimaryKey;

public class TupleSet implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3191495851408477607L;
	private List<Tuple> tuples;
	private int sinkId;
	private Serializable metadata;

	public TupleSet(int sinkId) {
		this.tuples = new ArrayList<Tuple>();
		this.sinkId = sinkId;
	}
	
	public void setMetadata(Serializable data){
		metadata = data;
	}
	
	public Serializable getMetadata(){
		return metadata;
	}

	public List<Tuple> getTupleSet() {
		return tuples;
	}

	public void addTuple(PrimaryKey key, long srcTxNum, long destTxNum,
			CachedRecord rec) {
		// Clone the record to prevent concurrent access from communication threads
		rec = new CachedRecord(rec);
		tuples.add(new Tuple(key, srcTxNum, destTxNum, rec));
	}

	public int sinkId() {
		return sinkId;
	}
	
	public int size() {
		return tuples.size();
	}
}
