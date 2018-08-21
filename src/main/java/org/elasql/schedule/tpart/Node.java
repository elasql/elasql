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
package org.elasql.schedule.tpart;

import java.util.ArrayList;
import java.util.List;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;


public class Node {
	private List<Edge> readEdges;
	private List<Edge> writeEdges;
	private List<Edge> writeBackEdges;
	private int partId;
	private TPartStoredProcedureTask task;
	private boolean sunk;
	// cache the number of record for each partition
	// this variable is used to speedup the insert process
	private int[] partitionRecordCount;  

	public Node(TPartStoredProcedureTask task) {
		this.task = task;
		readEdges = new ArrayList<Edge>();
		writeEdges = new ArrayList<Edge>();
		writeBackEdges = new ArrayList<Edge>();
	}
	
	public int[] getPartRecordCntArray(){
		return partitionRecordCount;
	}
	
	public void setPartRecordCntArray(int[] array){
		partitionRecordCount = array;
	}

	public List<Edge> getReadEdges() {
		return readEdges;
	}

	public List<Edge> getWriteEdges() {
		return writeEdges;
	}

	public List<Edge> getWriteBackEdges() {
		return writeBackEdges;
	}

	public void addReadEdges(Edge e) {
		readEdges.add(e);
	}

	public void addWriteEdges(Edge e) {
		writeEdges.add(e);
	}

	public void addWriteBackEdges(Edge e) {
		writeBackEdges.add(e);
	}

	public int getPartId() {
		return partId;
	}

	public void setPartId(int partId) {
		this.partId = partId;
	}

	public TPartStoredProcedureTask getTask() {
		return task;
	}

	public double getWeight() {
		return task.getWeight();
	}

	public boolean hasSunk() {
		return sunk;
	}

	public void setSunk(boolean sunk) {
		this.sunk = sunk;
	}

	public boolean isSinkNode() {
		return task == null;
	}

	public long getTxNum() {
		if (isSinkNode())
			return TPartCacheMgr.toSinkId(getPartId());
		else
			return task.getTxNum();
	}

	@Override
	public String toString() {
		return "[Node] Txn-id: " + getTask().getTxNum() + ", " + "read-edges: "
				+ readEdges + ", write-edges: " + writeEdges + ", weight: "
				+ task.getWeight();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj.getClass() != Node.class)
			return false;
		Node n = (Node) obj;
		return (n.task.getTxNum() == this.task.getTxNum() && n.partId == this.partId);
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = hash * 31
				+ (int) (this.task.getTxNum() ^ (this.task.getTxNum() >>> 32));
		hash = hash * 31 + this.partId;
		return hash;
	}
}
