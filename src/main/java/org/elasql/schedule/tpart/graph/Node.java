package org.elasql.schedule.tpart.graph;

import java.util.HashSet;
import java.util.Set;

public abstract class Node {

	private Set<Edge> writeEdges;
	private int partId;

	public Node() {
		writeEdges = new HashSet<Edge>();
	}
	
	public abstract double getWeight();
	
	public abstract boolean isSinkNode();
	
	public abstract long getTxNum();

	public Set<Edge> getWriteEdges() {
		return writeEdges;
	}

	public void addWriteEdges(Edge e) {
		writeEdges.add(e);
	}

	public int getPartId() {
		return partId;
	}

	public void setPartId(int partId) {
		this.partId = partId;
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
