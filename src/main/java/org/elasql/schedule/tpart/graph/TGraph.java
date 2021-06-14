package org.elasql.schedule.tpart.graph;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class TGraph {
	
	protected SinkNode[] sinkNodes;
	private List<TxNode> txNodes = new LinkedList<TxNode>();
	protected Map<PrimaryKey, TxNode> resPos = new HashMap<PrimaryKey, TxNode>();
	
	protected PartitionMetaMgr parMeta;
	
	// Statistics (lazy evaluation)
	private boolean isStatsCalculated = false;
	private int[] numOfNodes;
	private int imbalDis;
	private int[] remoteTxReads;
	private int totalRemoteTxReads;
	private int[] remoteSinkReads;
	private int totalRemoteSinkReads;

	public TGraph() {
		sinkNodes = new SinkNode[PartitionMetaMgr.NUM_PARTITIONS];
		for (int partId = 0; partId < sinkNodes.length; partId++)
			sinkNodes[partId] = new SinkNode(partId);
		parMeta = Elasql.partitionMetaMgr();
	}

	/**
	 * Insert a new tx node into the t-graph.
	 * 
	 * @param node
	 */
	public void insertTxNode(TPartStoredProcedureTask task, int assignedPartId) {
		TxNode node = new TxNode(task, assignedPartId);
		txNodes.add(node);
		
		// Establish forward pushing edges
		if (task.getReadSet() != null) {
			// create a read edge to the latest txn that writes that resource
			for (PrimaryKey res : task.getReadSet()) {

				Node targetNode;

				if (parMeta.isFullyReplicated(res))
					targetNode = sinkNodes[node.getPartId()];
				else
					targetNode = getResourcePosition(res);

				node.addReadEdges(new Edge(targetNode, res));
				targetNode.addWriteEdges(new Edge(node, res));
			}
		}
		
		// Update the resource locations
		if (task.getWriteSet() != null) {
			// update the resource position
			for (PrimaryKey res : task.getWriteSet())
				resPos.put(res, node);
		}
	}

	/**
	 * Write back all modified data records to their original partitions.
	 */
	public void addWriteBackEdge() {
		// XXX should implement different write back strategy
		for (Entry<PrimaryKey, TxNode> resPosPair : resPos.entrySet()) {
			PrimaryKey res = resPosPair.getKey();
			TxNode node = resPosPair.getValue();
			node.addWriteBackEdges(new Edge(sinkNodes[parMeta.getPartition(res)], res));
		}
		resPos.clear();
	}
	
	public void clear() {
		// clear the edges from sink nodes
		for (int i = 0; i < sinkNodes.length; i++)
			sinkNodes[i].getWriteEdges().clear();
		
		// remove all tx nodes
		txNodes.clear();
		
		// reset the statistics
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
	public Node getResourcePosition(PrimaryKey res) {
		if (resPos.containsKey(res))
			return resPos.get(res);
		return sinkNodes[parMeta.getPartition(res)];
	}

	public List<TxNode> getTxNodes() {
		return txNodes;
	}
	
	public TxNode getLastInsertedTxNode() {
		return txNodes.get(txNodes.size() - 1);
	}
	
	private void calculateStatistics() {
		if (isStatsCalculated)
			return;
		
		// Count the # of nodes in each partition
		numOfNodes = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		// Count how many remote read edges starting from each partition
		remoteTxReads = new int[PartitionMetaMgr.NUM_PARTITIONS];
		remoteSinkReads = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (TxNode node : txNodes) {
			int partId = node.getPartId();
			
			numOfNodes[partId]++;
			
			for (Edge edge : node.getReadEdges()) {
				if (partId != edge.getTarget().getPartId()) {
					if (edge.getTarget().isSinkNode()) {
						remoteSinkReads[partId]++;
					} else {
						remoteTxReads[partId]++;
					}
				}
					
			}
		}
		
		// Count imbalance distance = sum(|(count - avg)|)
		imbalDis = 0;
		int avg = txNodes.size() / PartitionMetaMgr.NUM_PARTITIONS;
		for (int numOfNode : numOfNodes)
			imbalDis += Math.abs(numOfNode - avg);
		
		// Count the total number of remote read edges
		totalRemoteTxReads = 0;
		for (int remoteTxRead : remoteTxReads)
			totalRemoteTxReads += remoteTxRead;
		totalRemoteSinkReads = 0;
		for (int remoteReadEdge : remoteSinkReads)
			totalRemoteSinkReads += remoteReadEdge;
		
		isStatsCalculated = true;
	}
	
	public int getImbalancedDis() {
		calculateStatistics();
		return imbalDis;
	}
	
	public int getRemoteTxReads() {
		calculateStatistics();
		return totalRemoteTxReads;
	}
	
	public int getRemoteSinkReads() {
		calculateStatistics();
		return totalRemoteSinkReads;
	}
	
	public Map<PrimaryKey, Node> getResourceNodeMap() {
		return new HashMap<PrimaryKey, Node>(resPos);
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
		sb.append("# of remote tx reads: ");
		for (int txRead : remoteTxReads)
			sb.append(String.format("%d ", txRead));
		sb.append("\n");
		sb.append("Total # of remote tx reads: " + totalRemoteTxReads + "\n");
		sb.append("# of remote sink reads: ");
		for (int sinkRead : remoteSinkReads)
			sb.append(String.format("%d ", sinkRead));
		sb.append("\n");
		sb.append("Total # of remote sink reads: " + totalRemoteSinkReads + "\n");
		sb.append("===============================================\n");
		
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		for (Node node : txNodes)
			sb.append(node + "\n");
		
		return sb.toString();
	}
}
