package org.elasql.schedule.tpart.sink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.elasql.schedule.tpart.Edge;
import org.elasql.schedule.tpart.Node;
import org.elasql.schedule.tpart.TGraph;
import org.elasql.schedule.tpart.TPartPartitioner;
import org.elasql.server.Elasql;
import org.elasql.server.task.tpart.TPartStoredProcedureTask;
import org.elasql.sql.RecordKey;

public class CacheOptimizedSinker extends Sinker {
	// private static long sinkPushTxNum;
	private static int sinkProcessId = 0;
	private int myId = Elasql.serverId();

	private NewTPartCacheMgr cm = (NewTPartCacheMgr) Elasql.cacheMgr();

	public CacheOptimizedSinker() {
		// the tx numbers of sink flush task are unused negative value
		// sinkPushTxNum = -(TPartPartitioner.NUM_PARTITIONS) - 1;
	}

	@Override
	public Iterator<TPartStoredProcedureTask> sink(TGraph graph, long txNum) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<TPartStoredProcedureTask> sink(TGraph graph) {

		// add write back edges
		graph.addWriteBackEdge();

		// create sunk plan list
		List<TPartStoredProcedureTask> plans;
		plans = createSunkPlan(graph);

		// clean up this sink's info
		graph.clearSinkNodeEdges();
		graph.removeSunkNodes();
		TPartPartitioner.costFuncCal.reset();

		sinkProcessId++;

		return plans.iterator();

	}

	/**
	 * Note: only sink the task that will be executed in this machine to result
	 * set.
	 * 
	 * @param node
	 * @return
	 */
	private List<TPartStoredProcedureTask> createSunkPlan(TGraph graph) {
		// long time = System.nanoTime();
		/*
		 * Prepare the cache info and sink plan.
		 */

		// // the version of record will read from remote site
		// List<CachedEntryKey> remoteFlags = new ArrayList<CachedEntryKey>();

		// the version of record that will be write back to local storage
		List<RecordKey> writeBackFlags = new ArrayList<RecordKey>();

		// create procedure tasks
		List<TPartStoredProcedureTask> localTasks = new LinkedList<TPartStoredProcedureTask>();
		// TPartSinkPushTask sinkPushTask = new TPartSinkPushTask(
		// new SinkPushProcedure(sinkPushTxNum, myId, sinkProcessId));
		// sinkPushTxNum--;

		// localTasks.add(sinkPushTask);

		for (Node node : graph.getNodes()) {
			// System.out.println("Tx: " + node.getTxNum());

			// task is local if the tx logic should be executed locally
			boolean taskIsLocal = (node.getPartId() == myId);
			boolean replicated = false;
			// if (!taskIsLocal && node.getWriteEdges().size() > 0) {
			// for (Edge edge : node.getWriteEdges()) {
			// int targetPart = edge.getTarget().getPartId();
			// if (targetPart == VanillaDdDb.serverId()) {
			// taskIsLocal = true;
			// replicated = true;
			// }
			// }
			// }
			long txNum = node.getTxNum();
			SunkPlan plan = new SunkPlan(sinkProcessId, taskIsLocal);
			node.getTask().setSunkPlan(plan);

			// readings
			// System.out.println("Read edges: " + txNum + " pid: "
			// + node.getPartId());
			for (Edge e : node.getReadEdges()) {
				// System.out.println("key:" + e.getResourceKey() + ",target:"
				// + e.getTarget().getTxNum());
				long srcTxn = e.getTarget().getTxNum();
				boolean isLocalResource = (e.getTarget().getPartId() == myId);
				if (taskIsLocal) {
					plan.addReadingInfo(e.getResourceKey(), srcTxn);
					// if (!isLocalResource)
					// remoteFlags.add(new CachedEntryKey(e.getResourceKey(),
					// srcTxn, node.getTxNum()));
					// else
					if (isLocalResource && e.getTarget().isSinkNode())
						plan.addSinkReadingInfo(e.getResourceKey());

				} else if (isLocalResource && e.getTarget().isSinkNode()) {
					// if is not local task and the source of the edge is sink
					// node add the push tag to sinkPushTask
					plan.addSinkPushingInfo(e.getResourceKey(), node.getPartId(),
							NewTPartCacheMgr.getPartitionTxnId(myId), node.getTxNum());
				}
			}

			// for every local task, push to remote if dest. node not in local
			// System.out.println("Write edges: ");
			if (taskIsLocal) {
				for (Edge e : node.getWriteEdges()) {
					// System.out.println("key:" + e.getResourceKey() +
					// ",target:"
					// + e.getTarget().getTxNum());
					int targetServerId = e.getTarget().getPartId();
					if (targetServerId != myId)
						plan.addPushingInfo(e.getResourceKey(), targetServerId, txNum, e.getTarget().getTxNum());

					plan.addWritingInfo(e.getResourceKey(), e.getTarget().getTxNum());
				}
			}

			// write back
			// System.out.println("Write back edges: ");
			if (node.getWriteBackEdges().size() > 0 && !replicated) {
				for (Edge e : node.getWriteBackEdges()) {
					// System.out.println("key:" + e.getResourceKey() +
					// ",target:"
					// + e.getTarget().getTxNum());

					int targetServerId = e.getTarget().getPartId();
					RecordKey k = e.getResourceKey();

					if (taskIsLocal) {
						if (targetServerId == myId) {
							// tell the task to write back local
							plan.addLocalWriteBackInfo(k);
							writeBackFlags.add(k);
						} else {
							// push the data if write back to remote
							plan.addPushingInfo(k, targetServerId, txNum,
									NewTPartCacheMgr.getPartitionTxnId(targetServerId));

						}
						plan.addWritingInfo(e.getResourceKey(), NewTPartCacheMgr.getPartitionTxnId(targetServerId));
					} else {
						if (targetServerId == myId) {
							// remoteFlags.add(new CachedEntryKey(k, txNum, node
							// .getTxNum()));
							writeBackFlags.add(k);
							plan.addLocalWriteBackInfo(k);
						}
					}
				}
			}

			/*
			 * For each tx node, create a procedure task for it. The task will
			 * be scheduled locally if 1) the task is partitioned into current
			 * server or 2) the task needs to write back records to this server.
			 */
			if (taskIsLocal || plan.hasLocalWriteBack() || plan.hasSinkPush()) {
				// System.out.println("Task: " + node.getTxNum());
				localTasks.add(node.getTask());
			}

			node.setSunk(true);
		}

		// // set remote flags
		// for (CachedEntryKey key : remoteFlags) {
		// VanillaDdDb.tPartCacheMgr().setRemoteFlag(key.getRecordKey(),
		// key.getSource(), key.getDestination());
		// }

		// set write back flags
		for (RecordKey key : writeBackFlags) {
			cm.setWriteBackInfo(key, sinkProcessId);
		}

		// System.out.println(sinkProcessId + " sunk execution time: "
		// + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
		return localTasks;
	}
}
