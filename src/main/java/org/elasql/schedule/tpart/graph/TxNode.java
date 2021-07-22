package org.elasql.schedule.tpart.graph;

import java.util.HashSet;
import java.util.Set;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;

public class TxNode extends Node {
	
	private Set<Edge> readEdges;
	private Set<Edge> writeBackEdges;
	
	private TPartStoredProcedureTask txTask;
	
	public TxNode(TPartStoredProcedureTask txTask, int partId) {
		this.txTask = txTask;
		readEdges = new HashSet<Edge>();
		writeBackEdges = new HashSet<Edge>();
		setPartId(partId);
	}

	public Set<Edge> getReadEdges() {
		return readEdges;
	}

	public Set<Edge> getWriteBackEdges() {
		return writeBackEdges;
	}

	public void addReadEdges(Edge e) {
		readEdges.add(e);
	}

	public void addWriteBackEdges(Edge e) {
		writeBackEdges.add(e);
	}

	@Override
	public double getWeight() {
		return txTask.getWeight();
	}

	@Override
	public boolean isSinkNode() {
		return false;
	}

	@Override
	public long getTxNum() {
		return txTask.getTxNum();
	}
	
	public TPartStoredProcedureTask getTask() {
		return txTask;
	}

	@Override
	public String toString() {
		return "[Node] Txn-id: " + getTxNum() + ", " + "read-edges: "
				+ readEdges + ", write-edges: " + getWriteEdges() + ", weight: "
				+ getWeight() + ", part id: " + getPartId();
	}
}
