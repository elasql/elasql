package org.elasql.server.migration.heatgraph;

import org.elasql.server.migration.MigrationManager;

public class OutEdge implements Comparable<OutEdge> {
	
	private int weight;
	private Vertex opposite;

	public OutEdge(Vertex opposite) {
		this(opposite, 1);
	}

	OutEdge(Vertex opposite, int weight) {
		this.opposite = opposite;
		this.weight = weight;
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

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!this.getClass().equals(obj.getClass()))
			return false;
		
		OutEdge e = (OutEdge) obj;
		
		return this.weight == e.weight &&
				this.opposite.getId() == e.opposite.getId();
	}
}
