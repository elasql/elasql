package org.elasql.schedule.tpart.graph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class GraphDumper {

	private static class DumpedEdge {

		public final long source;
		public final long dest;

		public DumpedEdge(long source, long dest) {
			this.source = source;
			this.dest = dest;
		}

		@Override
		public int hashCode() {
			return (int) (source * 17 + dest * 17 + 37);
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof DumpedEdge))
				return false;
			DumpedEdge e = (DumpedEdge) o;
			return source == e.source && dest == e.dest;
		}

	}

	public static void dumpToFile(File file, TGraph graph) {
		List<TxNode> nodes = graph.getTxNodes();
		Map<DumpedEdge, Integer> edges = new HashMap<DumpedEdge, Integer>();

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			// Print the number of nodes
			writer.write(String.format("%d", nodes.size() + PartitionMetaMgr.NUM_PARTITIONS));
			writer.newLine();

			// Print the information of sink nodes
			for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
				writer.write(String.format("%d %d", TPartCacheMgr.toSinkId(partId), partId));
				writer.newLine();
			}

			// Print the partition of each node
			for (TxNode node : nodes) {
				long txNum = node.getTxNum();
				
				writer.write(String.format("%d %d", txNum, node.getPartId()));
				writer.newLine();
				
				// Read edges
				for (Edge edge : node.getReadEdges()) {
					edges.put(new DumpedEdge(edge.getTarget().getTxNum(), txNum),
							Integer.parseInt((String) edge.getResourceKey().getKeyVal("ycsb_id").asJavaVal()));
				}
				
				// Write edges
				for (Edge edge : node.getWriteEdges()) {
					edges.put(new DumpedEdge(txNum, edge.getTarget().getTxNum()),
							Integer.parseInt((String) edge.getResourceKey().getKeyVal("ycsb_id").asJavaVal()));
				}
				
				// Write back edges
				for (Edge edge : node.getWriteBackEdges()) {
					edges.put(new DumpedEdge(txNum, edge.getTarget().getTxNum()),
							Integer.parseInt((String) edge.getResourceKey().getKeyVal("ycsb_id").asJavaVal()));
				}
			}

			// Print the edges
			writer.write(String.format("%d", edges.size()));
			writer.newLine();
			
			for (Entry<DumpedEdge, Integer> entry : edges.entrySet()) {
				writer.write(String.format("%d %d %d", entry.getKey().source, entry.getKey().dest,
						entry.getValue().intValue()));
				writer.newLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
