package org.elasql.migration.planner.clay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class Partition implements Comparable<Partition> {

	private class VertexWeightComparator implements Comparator<Vertex> {
		@Override
		public int compare(Vertex v1, Vertex v2) {
			if (v1.getVertexWeight() > v2.getVertexWeight())
				return 1;
			else if (v1.getVertexWeight() < v2.getVertexWeight())
				return -1;
			return 0;
		}
	}
	
	private int partId;
	private double localLoad;
	private double crossPartLoad;
	private List<Vertex> vertices;
	private double multiPartsCost;

	Partition(int partId, double multiPartsCost) {
		this.partId = partId;
		this.localLoad = 0;
		this.crossPartLoad = 0;
		this.vertices = new ArrayList<Vertex>();
		this.multiPartsCost = multiPartsCost;
	}

	int getPartId() {
		return partId;
	}
	
	double getLocalLoad() {
		return localLoad;
	}
	
	double getCrossPartLoad() {
		return crossPartLoad;
	}

	double getTotalLoad() {
		return localLoad + multiPartsCost * crossPartLoad;
	}

	void addVertex(Vertex v) {
		if (v.getPartId() != partId)
			throw new RuntimeException("Vertex " + v + " is not in partition " + partId);
		
		localLoad += v.getVertexWeight();
		for (OutEdge e : v.getOutEdges())
			if (e.getOpposite().getPartId() != partId)
				crossPartLoad += e.getWeight();
		vertices.add(v);
	}

	Vertex getHotestVertex() {
		return Collections.max(vertices, new VertexWeightComparator());
	}

	Vertex[] getFirstTen() {
		Vertex[] vtxs = new Vertex[10];
		Collections.sort(vertices, new VertexWeightComparator());
		int l = vertices.size();
		int j = 0;
		for (int i = l - 1; i >= l - 10; i--) {
			vtxs[j++] = vertices.get(i);
		}
		return vtxs;
	}

	@Override
	public int compareTo(Partition other) {
		double load = getTotalLoad();
		double otherLoad = other.getTotalLoad();
		
		if (load > otherLoad)
			return 1;
		else if (load < otherLoad)
			return -1;
		return 0;
	}

}
