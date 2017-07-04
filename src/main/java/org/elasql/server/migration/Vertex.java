package org.elasql.server.migration;

import java.util.HashMap;
import java.util.Map.Entry;

public class Vertex {

	private HashMap<Integer, Integer> edge;
	private int weight;
	private int id;

	public Vertex(int id) {
		this.id = id;
		this.weight = 1;
		edge = new HashMap<Integer, Integer>();
	}

	public int getId() {
		return id;
	}

	public void add() {
		this.weight++;
	}

	public void addEdge(Integer d) {
		Integer w = edge.get(d);

		if (w == null)
			edge.put(d, new Integer(1));
		else
			edge.put(d, new Integer(w+1));
	}

	public void clear() {
		this.weight = 0;
		edge.clear();
	}

	public int getWeight() {
		return this.weight;
	}
	public HashMap<Integer, Integer> getEdge(){
		return this.edge;
	}

	public String toString() {
		String str = "Vertex id : " + this.id + " Weight :" + this.weight + "\n";
		for (Entry<Integer, Integer> e : edge.entrySet()) {
			str = str + e.getKey() + " w: " + e.getValue() + "\n ";
		}

		return str;
	}
}
