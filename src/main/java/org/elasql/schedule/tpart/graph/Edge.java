package org.elasql.schedule.tpart.graph;

import org.elasql.sql.PrimaryKey;

public class Edge {

	private Node target;
	private PrimaryKey resource;

	public Edge(Node target, PrimaryKey res) {
		this.target = target;
		this.resource = res;
	}

	public Node getTarget() {
		return target;
	}

	public PrimaryKey getResourceKey() {
		return resource;
	}

	@Override
	public String toString() {
		return String.format("{Resource: %s, from/to tx: %d, part: %d}", resource, target.getTxNum(),
				target.getPartId());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!obj.getClass().equals(this.getClass()))
			return false;
		
		Edge e = (Edge) obj;
		
		return this.resource.equals(e.resource) && this.target.equals(e.target);
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + resource.hashCode();
		hash = hash * 31 + target.hashCode();
		return hash;
	}
}
