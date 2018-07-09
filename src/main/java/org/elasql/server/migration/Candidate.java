package org.elasql.server.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.elasql.server.migration.Vertex.OutEdge;

public class Candidate {

	private class Neighbor implements Comparable<Neighbor> {
		long weight;
		int id;
		int partId;

		public Neighbor(int id, int partId, int weight) {
			this.id = id;
			this.partId = partId;
			this.weight = weight;
		}

		@Override
		public int compareTo(Neighbor other) {
			if (this.weight > other.weight)
				return 1;
			else if (this.weight < other.weight)
				return -1;
			return 0;
		}

	}

	private HashMap<Integer, Neighbor> neighbors;
	private HashSet<Integer> candidateIds;
	private double load;

	public Candidate() {
		this.load = 0;
		this.candidateIds = new HashSet<Integer>();
		this.neighbors = new HashMap<Integer, Neighbor>();
	}

	public void addNeighbor(Integer id, int partId, int weight) {

		// Avoid self loop
		if (candidateIds.contains(id))
			return;

		Neighbor w = neighbors.get(id);
		if (w == null)
			neighbors.put(id, new Neighbor(id, partId, weight));
		else
			w.weight += weight;
	}

	public void addCandidate(Vertex v) {
		load += v.getVertexWeight();
		candidateIds.add(v.getId());
		// Naive version don't concider other partition node
		for (OutEdge o : v.getEdge().values())
			if (o.partId == v.getPartId())
				addNeighbor(o.id, o.partId, o.weight);
		// once expend this node remove it form neighbor
		neighbors.remove(v.getId());
	}

	public int getHotestNeighbor() {
		return Collections.max(neighbors.values()).id;
	}
	
	public boolean hasNeighbor() {
		return !neighbors.isEmpty();
	}

	public HashSet<Integer> getCandidateIds() {
		return candidateIds;
	}

	@Override
	public String toString() {
		String str = "Load : " + this.load + " Neighbor : " + neighbors.size() + "\n";
		str += "Candidate Id : ";
		ArrayList<Integer> cc = new ArrayList<Integer>(candidateIds);
		Collections.sort(cc);
		for (int id : cc)
			str += ", " + id;
		/*
		 * for (int id : candidateIds) if (id*MigrationManager.dataRange >
		 * 100000) str += "Somethigs Wrong Tuple " + id + "in candidateIds\n";
		 */
		str += "\n";
		return str;
	}

}
