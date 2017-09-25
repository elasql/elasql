package org.elasql.schedule.tpart;

import org.elasql.sql.RecordKey;

public class Edge {

	private Node target;
	private RecordKey resource;

	public Edge(Node target, RecordKey res) {
		this.target = target;
		this.resource = res;
	}

	public Node getTarget() {
		return target;
	}

	public RecordKey getResourceKey() {
		return resource;
	}

	@Override
	public String toString() {
		return String.format("{Resource: %s, from/to tx: %d, part: %d}", resource, target.getTxNum(),
				target.getPartId());
	}
}
