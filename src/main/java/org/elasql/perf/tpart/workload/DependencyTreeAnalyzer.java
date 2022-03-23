package org.elasql.perf.tpart.workload;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A tool to record and analyze the dependency tree of entire workload. It generates
 * the features that {@code FeatureExtractor} needs by iterating the tree. Note that
 * this tool is not thread-safe.
 * 
 * @author Yu-Shan Lin
 */
public class DependencyTreeAnalyzer {
	
	private static class TreeNode {
		Long txNum; // We use Long instead of long because most of data are stored as Long
		int depth;
		
		TreeNode(Long txNum, int depth) {
			this.txNum = txNum;
			this.depth = depth;
		}
	}
	
	private Map<Long, Set<Long>> dependencyTree = new ConcurrentHashMap<Long, Set<Long>>();
	private Queue<TreeNode> bfsQueue = new ArrayDeque<TreeNode>();
	
	public void addTransaction(long txNum, Set<Long> dependentTxns) {
		dependencyTree.put(txNum, dependentTxns);
	}
	
	public void onTransactionCommit(long txNum) {
		dependencyTree.remove(txNum);
	}
	
	public void addDependencyTreeFeatures(long txNum, TransactionFeatures.Builder builder) {
		int maxDepth = 0;
		int firstLayerCount = 0;
		int totalCount = 0;
		
		// Breadth-First Search (BFS)
		
		// Ensure the clearance
		bfsQueue.clear(); 
		
		// Add the dependent transactions to the queue
		Set<Long> dependencies = dependencyTree.get(txNum);
		for (Long dependentTx : dependencies)
			bfsQueue.add(new TreeNode(dependentTx, 1));
		
		// Iterate all dependent transactions
		while (!bfsQueue.isEmpty()) {
			TreeNode node = bfsQueue.remove();
			
			dependencies = dependencyTree.get(node.txNum);
			if (dependencies != null) {
				// Update the statistics
				totalCount++;
				if (node.depth == 1)
					firstLayerCount++;
				if (node.depth > maxDepth)
					maxDepth = node.depth;
				
				// Add the following depedent transactions to the queue
				for (Long dependentTx : dependencies)
					bfsQueue.add(new TreeNode(dependentTx, node.depth + 1));
			}
		}
		
		// Record the features
		builder.addFeature("Dependency - Max Depth", maxDepth);
		builder.addFeature("Dependency - First Layer Tx Count", firstLayerCount);
		builder.addFeature("Dependency - Total Tx Count", totalCount);
	}
}
