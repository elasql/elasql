package org.elasql.schedule.tpart.graph;

import org.elasql.sql.PrimaryKey;
import org.elasql.server.Elasql;

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

	// MODIFIED: Check whether the edge is remote read
	/**
	 * Return a boolean indicating whether the edge is remoted or not
	 * @return
	 */
	public Boolean isRemoteRead(){
		return target.getPartId() == Elasql.serverId();
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
