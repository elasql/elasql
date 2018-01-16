package org.elasql.schedule.tpart;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class TGraph {
	private List<Node> nodes = new LinkedList<Node>();
	/// XXX A Map indicate where are the records' position
	protected Map<RecordKey, Node> resPos = new HashMap<RecordKey, Node>();
	protected Node[] sinkNodes;
	protected PartitionMetaMgr parMeta;
	
	// Statistics (lazy evaluation)
	private boolean isStatsCalculated = false;
	private int[] numOfNodes;
	private int imbalDis;
	private int[] remoteReadEdges;
	private int totalRemoteReads;

	public TGraph() {
		sinkNodes = new Node[PartitionMetaMgr.NUM_PARTITIONS];
		for (int i = 0; i < sinkNodes.length; i++) {
			Node node = new Node(null);
			node.setPartId(i);
			sinkNodes[i] = node;
		}
		parMeta = Elasql.partitionMetaMgr();
	}

	/**
	 * Insert the new node into the t-graph.
	 * 
	 * @param node
	 */
	public void insertNode(Node node) {
		if (node.getTask() == null)
			return;

		nodes.add(node);

		if (node.getTask().getReadSet() != null) {
			// create a read edge to the latest txn that writes that resource
			for (RecordKey res : node.getTask().getReadSet()) {

				Node targetNode;

				if (parMeta.isFullyReplicated(res))
					targetNode = sinkNodes[node.getPartId()];
				else
					targetNode = getResourcePosition(res);

				node.addReadEdges(new Edge(targetNode, res));
				targetNode.addWriteEdges(new Edge(node, res));
			}
		}

		if (node.getTask().getWriteSet() != null) {
			// update the resource position
			for (RecordKey res : node.getTask().getWriteSet())
				resPos.put(res, node);
		}
	}

	/**
	 * Write back all modified data records to their original partitions.
	 */
	public void addWriteBackEdge() {
		// XXX should implement different write back strategy
		for (Entry<RecordKey, Node> resPosPair : resPos.entrySet()) {
			RecordKey res = resPosPair.getKey();
			Node node = resPosPair.getValue();

			if (node.getTask() != null)
				node.addWriteBackEdges(new Edge(sinkNodes[parMeta.getCurrentLocation(res)], res));
		}
		resPos.clear();
	}

	public void clearSinkNodeEdges() {
		for (int i = 0; i < sinkNodes.length; i++)
			sinkNodes[i].getWriteEdges().clear();
	}

	public void removeSunkNodes() {
		nodes.clear();
		isStatsCalculated = false;
	}

	/**
	 * Get the node that produce the latest version of specified resource.
	 * 
	 * @param
	 * @return The desired node. If the resource has not been created a new
	 *         version since last sinking, the partition that own the resource
	 *         will be return in a Node format.
	 */
	public Node getResourcePosition(RecordKey res) {
		if (resPos.containsKey(res))
			return resPos.get(res);
		return sinkNodes[parMeta.getCurrentLocation(res)];
	}

	public List<Node> getNodes() {
		return nodes;
	}
	
	public Node getLastInsertedNode() {
		return nodes.get(nodes.size() - 1);
	}
	
	private void calculateStatistics() {
		if (isStatsCalculated)
			return;
		
		// Count the # of nodes in each partition
		numOfNodes = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		// Count how many remote read edges starting from each partition
		remoteReadEdges = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (Node node : nodes) {
			int partId = node.getPartId();
			
			numOfNodes[partId]++;
			
			for (Edge edge : node.getReadEdges()) {
				if (partId != edge.getTarget().getPartId())
					remoteReadEdges[partId]++;
			}
		}
		
		// Count imbalance distance = sum(|(count - avg)|)
		imbalDis = 0;
		int avg = nodes.size() / PartitionMetaMgr.NUM_PARTITIONS;
		for (int numOfNode : numOfNodes)
			imbalDis += Math.abs(numOfNode - avg);
		
		// Count the total number of remote read edges
		totalRemoteReads = 0;
		for (int remoteReadEdge : remoteReadEdges)
			totalRemoteReads += remoteReadEdge;
		
		isStatsCalculated = true;
	}
	
	public int getImbalancedDis() {
		calculateStatistics();
		return imbalDis;
	}
	
	public int getNumberOfRemotes() {
		calculateStatistics();
		return totalRemoteReads;
	}
	
	public Map<RecordKey, Node> getResourceNodeMap() {
		return new HashMap<RecordKey, Node>(resPos);
	}
	
	public String getStatistics() {
		StringBuilder sb = new StringBuilder();
		
		calculateStatistics();
		
		sb.append("============= T-Graph Statistics ==============\n");
		sb.append("# of nodes: ");
		for (int numOfNode : numOfNodes)
			sb.append(String.format("%d ", numOfNode));
		sb.append("\n");
		sb.append("Imbalance distance: " + imbalDis + "\n");
		sb.append("# of remote read edges: ");
		for (int remoteRead : remoteReadEdges)
			sb.append(String.format("%d ", remoteRead));
		sb.append("\n");
		sb.append("Total # of remote read edges: " + totalRemoteReads + "\n");
		sb.append("===============================================\n");
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		for (Node node : nodes)
			sb.append(node + "\n");
		
		return sb.toString();
	}
}
