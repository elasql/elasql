package org.elasql.schedule.tpart.sink;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.procedure.tpart.TransactionGraph;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
// import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class Sinker {
	
	protected PartitionMetaMgr parMeta;
	protected int myId = Elasql.serverId();
	protected static int sinkProcessId = 0;

	private TransactionGraph txnGraph = Elasql.getTransactionGraph(); 

	public Sinker() {
		parMeta = Elasql.partitionMetaMgr();
	}
	
	public Iterator<TPartStoredProcedureTask> sink(TGraph graph) {
		
		// add write back edges
		graph.addWriteBackEdge();

		// create sunk plan list
		List<TPartStoredProcedureTask> plans;
		plans = createSunkPlan(graph);

		// clean up this sink's info
		graph.clear();

		sinkProcessId++;

		return plans.iterator();
	}
	// MODIFIED:
	/**
	 * Generate the dependency graph for current node
	 * @param node
	 */
	private void generateDependencyGraph(TxNode node){
		// @VERSION1
		// for(PrimaryKey key : node.getTask().getReadSet()){
		// 	node.getTask().getProcedure().addDependenTxns(ConservativeOrderedCcMgr.checkPreviousWaitingTxns(key, true));
		// }
		
		// for(PrimaryKey key : node.getTask().getWriteSet()){
		// 	node.getTask().getProcedure().addDependenTxns(ConservativeOrderedCcMgr.checkPreviousWaitingTxns(key, false));
		// }

		// @VERSION2
		// node.getTask().getProcedure().addDependenTxns(txnGraph.checkPreviousWaitingTxnSet(node.getTask().getReadSet(), true));
		// node.getTask().getProcedure().addDependenTxns(txnGraph.checkPreviousWaitingTxnSet(node.getTask().getWriteSet(), false));

		// @VERSION3
		Set<Long> dependentSet = txnGraph.generateDependencyGraph(node.getTask().getReadSet(), node.getTask().getWriteSet(), node.getTxNum());
		node.getTask().getProcedure().addDependenTxns(dependentSet);
	}

	// MODIFIED:
	/**
	 * Add lock requests to queue for building dependency graph.
	 * @param node
	 */
	// private void addRWLockQueue(TxNode node){
	// 	// @VERSION1
	// 	// for(PrimaryKey key : node.getTask().getReadSet()){
	// 	// 	ConservativeOrderedCcMgr.getLockTbl().addSLockRequest(key, node.getTxNum());
	// 	// }

	// 	// for(PrimaryKey key : node.getTask().getWriteSet()){
	// 	// 	ConservativeOrderedCcMgr.getLockTbl().addXLockRequest(key, node.getTxNum());
	// 	// }

	// 	// @VERSION2
	// 	txnGraph.addSLockRequests(node.getTask().getReadSet(), node.getTxNum());
	// 	txnGraph.addXLockRequests(node.getTask().getWriteSet(), node.getTxNum());
	// }
	
	protected List<TPartStoredProcedureTask> createSunkPlan(TGraph graph) {
		List<TPartStoredProcedureTask> localTasks = new LinkedList<TPartStoredProcedureTask>();

		// Build a local execution plan for each transaction node
		for (TxNode node : graph.getTxNodes()) {
			// Debug
//			System.out.println(String.format("Node %d: %s (writeback: %d)", node.getTxNum(),
//					node.getTask().getProcedure().getClass().getSimpleName(), node.getWriteBackEdges().size()));
			
			// Check if this node is the master node
			boolean isHereMaster = (node.getPartId() == myId);
			// MODIFIED: Correspond to the changes of constructor
			SunkPlan plan = new SunkPlan(sinkProcessId, isHereMaster, node);

			// Generate reading plans
			generateReadingPlans(plan, node);
			
			// Generate writing plans
			generateWritingPlans(plan, node);

			// Generate write back (to sinks) plans
			generateWritingBackPlans(plan, node);
			
			// MODIFIED: Generate dependency graph and add lock requests to queue for building dependency graph.
			generateDependencyGraph(node);
			// addRWLockQueue(node);
			
			
			// Decide if the local node should execute this plan
			if (plan.shouldExecuteHere()) {
				// Debug
//				System.out.println(String.format("Tx.%d plan: %s", node.getTxNum(), plan));
				node.getTask().decideExceutionPlan(plan);	
				localTasks.add(node.getTask());
			}
		}
		
		return localTasks;
	}
	
	protected void generateReadingPlans(SunkPlan plan, TxNode node) {
		for (Edge e : node.getReadEdges()) {
			long srcTxn = e.getTarget().getTxNum();
			boolean isLocalResource = (e.getTarget().getPartId() == myId);
			
			if (plan.isHereMaster()) {
				plan.addReadingInfo(e.getResourceKey(), srcTxn);
				
				// Read from the local storage (sink)
				if (isLocalResource && e.getTarget().isSinkNode()) {
					plan.addSinkReadingInfo(e.getResourceKey());
				}

			} else if (isLocalResource && e.getTarget().isSinkNode()) {
				// I'm not the master node, but I have the required resource in my storage (sink).
				// Add a push plan
				plan.addSinkPushingInfo(e.getResourceKey(), node.getPartId(), node.getTxNum());
			}
		}
	}
	
	protected void generateWritingPlans(SunkPlan plan, TxNode node) {
		// do one of the following:
		// 1. Write (pass) to a local transaction
		// 2. Push to a remote transaction
		if (plan.isHereMaster()) {
			for (Edge e : node.getWriteEdges()) {
				int targetServerId = e.getTarget().getPartId();
				if (targetServerId != myId)
					plan.addPushingInfo(e.getResourceKey(), targetServerId, e.getTarget().getTxNum());
				else
					plan.addLocalPassingTarget(e.getResourceKey(), e.getTarget().getTxNum());
			}
		}
	}
	
	// Writing back (to sinks)
	protected void generateWritingBackPlans(SunkPlan plan, TxNode node) {
		for (Edge e : node.getWriteBackEdges()) {
			int dataWriteBackPos = e.getTarget().getPartId();
			PrimaryKey k = e.getResourceKey();
			
			// For any node (which may not be the master node),
			// if it is the destination of a write-back,
			// add this info to the plan.
			if (dataWriteBackPos == myId) {
				// tell the task to write back local
				plan.addLocalWriteBackInfo(k);
			} else if (plan.isHereMaster()) { // XXX: Untested
				// push the write-back data to the remote node
				plan.addPushingInfo(k, dataWriteBackPos, TPartCacheMgr.toSinkId(dataWriteBackPos));
			}
		}
	}
}
