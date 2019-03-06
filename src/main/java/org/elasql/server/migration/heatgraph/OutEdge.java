package org.elasql.server.migration.heatgraph;

import org.elasql.server.migration.MigrationManager;

public class OutEdge implements Comparable<OutEdge> {
	
	private int weight;
	private Vertex opposite;

	public OutEdge(Vertex opposite) {
		this.opposite = opposite;
		this.weight = 1;
	}
	
	public void incrementWeight() {
		this.weight++;
	}
	
	public int getWeight() {
		return weight;
	}
	
	public double getNormalizedWeight() {
		return (double) weight / MigrationManager.MONITORING_TIME;
	}
	
	public Vertex getOpposite() {
		return opposite;
	}

	@Override
	public int compareTo(OutEdge other) {
		if (this.weight > other.weight)
			return 1;
		else if (this.weight < other.weight)
			return -1;
		return 0;
	}
}
