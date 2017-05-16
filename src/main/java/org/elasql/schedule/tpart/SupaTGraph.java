package org.elasql.schedule.tpart;

import java.util.Map.Entry;

import org.elasql.sql.RecordKey;

public class SupaTGraph extends TGraph {
	public SupaTGraph() {
		super();
	}

	@Override
	/**
	 * Write back to where TGraph assigned
	 */
	public void addWriteBackEdge() {
		// XXX should implement different write back strategy

		for (Entry<RecordKey, Node> resPosPair : resPos.entrySet()) {
			RecordKey res = resPosPair.getKey();
			Node node = resPosPair.getValue();

			// null means it is sink node
			if (node.getTask() != null)
				node.addWriteBackEdges(new Edge(sinkNodes[node.getPartId()], res));

		}
		resPos.clear();
	}
}