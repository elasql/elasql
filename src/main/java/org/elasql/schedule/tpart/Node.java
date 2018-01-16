package org.elasql.schedule.tpart;

import java.util.HashSet;
import java.util.Set;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;

public class Node {
	private Set<Edge> readEdges;
	private Set<Edge> writeEdges;
	private Set<Edge> writeBackEdges;
	private int partId;
	private TPartStoredProcedureTask task;
	private boolean sunk;

	public Node(TPartStoredProcedureTask task) {
		this.task = task;
		readEdges = new HashSet<Edge>();
		writeEdges = new HashSet<Edge>();
		writeBackEdges = new HashSet<Edge>();
	}

	public Set<Edge> getReadEdges() {
		return readEdges;
	}

	public Set<Edge> getWriteEdges() {
		return writeEdges;
	}

	public Set<Edge> getWriteBackEdges() {
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
		return "[Node] Txn-id: " + getTxNum() + ", " + "read-edges: "
				+ readEdges + ", write-edges: " + writeEdges + ", weight: "
				+ task.getWeight() + ", part id: " + partId;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj.getClass() != Node.class)
			return false;
		Node n = (Node) obj;
		return (n.getTxNum() == getTxNum() && n.partId == this.partId);
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = hash * 31
				+ (int) (getTxNum() ^ (getTxNum() >>> 32));
		hash = hash * 31 + this.partId;
		return hash;
	}
}
