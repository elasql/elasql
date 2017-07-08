package org.elasql.server.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Partition implements Comparable<Partition> {
	private List<Vertex> Vertexs;
	private double load;
	private double edgeLoad;
	private int id;

	public Partition(int id) {
		this.load = 0;
		this.edgeLoad = 0;
		this.id = id;
		Vertexs = new ArrayList<Vertex>();
	}

	public int getId() {
		return this.id;
	}

	public double getLoad() {
		return load;
	}

	public double getEdgeLoad() {
		return edgeLoad;
	}

	public double getTotalLoad() {
		return load + MigrationManager.BETA * edgeLoad;
	}

	public void addVertex(Vertex v) {
		this.load += v.getVertexWeight();
		this.edgeLoad += v.getEdgeWeight();
		Vertexs.add(v);
	}

	public Vertex getHotestVertex() {
		return Collections.max(Vertexs);
	}

	public Vertex[] getFirstTen() {
		Vertex[] vtxs = new Vertex[10];
		Collections.sort(Vertexs);
		int l = Vertexs.size();
		int j = 0;
		for (int i = l - 1; i >= l - 10; i--) {
			vtxs[j++] = Vertexs.get(i);
		}
		return vtxs;
	}

	@Override
	public int compareTo(Partition other) {
		if (this.load > other.load)
			return 1;
		else if (this.load < other.load)
			return -1;
		return 0;
	}

}
