package org.elasql.migration.planner.clay;

class OutEdge implements Comparable<OutEdge> {
	
	private int weight;
	private Vertex opposite;

	OutEdge(Vertex opposite) {
		this(opposite, 1);
	}

	OutEdge(Vertex opposite, int weight) {
		this.opposite = opposite;
		this.weight = weight;
	}
	
	void incrementWeight() {
		this.weight++;
	}
	
	int getWeight() {
		return weight;
	}
	
	Vertex getOpposite() {
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
				this.opposite.getKey().equals(e.opposite.getKey());
	}
}
