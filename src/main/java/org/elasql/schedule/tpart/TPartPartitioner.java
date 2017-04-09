package org.elasql.schedule.tpart;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.Scheduler;
import org.elasql.schedule.tpart.sink.Sinker;
import org.elasql.server.Elasql;
import org.elasql.server.task.tpart.TPartStoredProcedureTask;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class TPartPartitioner extends Task implements Scheduler {
	public static final int NUM_PARTITIONS;

	public static CostFunctionCalculator costFuncCal;

	private static Logger logger = Logger.getLogger(TPartPartitioner.class.getName());

	private static final Class<?> FACTORY_CLASS, COST_FUNC_CLASS;

	private static final int NUM_TASK_PER_SINK;

	private static final int HAS_REORDERING;

	private List<TPartStoredProcedureTask> remoteTasks = new LinkedList<TPartStoredProcedureTask>();

	private TPartStoredProcedureFactory factory;

	private PartitionMetaMgr parMetaMgr = Elasql.partitionMetaMgr();

	static {
		FACTORY_CLASS = ElasqlProperties.getLoader().getPropertyAsClass(
				TPartPartitioner.class.getName() + ".FACTORY_CLASS", null, TPartStoredProcedureFactory.class);
		if (FACTORY_CLASS == null)
			throw new RuntimeException("Factory property is empty");

		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPartitioner.class.getName() + ".NUM_PARTITIONS", 1);

		HAS_REORDERING = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPartitioner.class.getName() + ".HAS_REORDERING", 0);

		COST_FUNC_CLASS = ElasqlProperties.getLoader().getPropertyAsClass(
				TPartPartitioner.class.getName() + ".COST_FUNC_CLS", null, CostFunctionCalculator.class);
		if (COST_FUNC_CLASS == null)
			throw new RuntimeException("Cost Fun property is empty");

		try {
			costFuncCal = (CostFunctionCalculator) COST_FUNC_CLASS.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		NUM_TASK_PER_SINK = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPartitioner.class.getName() + ".NUM_TASK_PER_SINK", 10);

	}

	private BlockingQueue<StoredProcedureCall> spcQueue;
	private NodeInserter inserter;
	private Sinker sinker;
	private TGraph graph;

	public TPartPartitioner(NodeInserter inserter, Sinker sinker, TGraph graph) {
		this.inserter = inserter;
		this.sinker = sinker;
		this.graph = graph;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();

		try {

			factory = (TPartStoredProcedureFactory) FACTORY_CLASS.newInstance();

		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

	}

	public void schedule(StoredProcedureCall... calls) {
		for (StoredProcedureCall call : calls)
			spcQueue.add(call);

		/*
		 * Deprecated for (int i = 0; i < calls.length; i++) {
		 * StoredProcedureCall call = calls[i]; // log request
		 * 
		 * TPartStoredProcedureTask spt; if (call.isNoOpStoredProcCall()) { spt
		 * = new TPartStoredProcedureTask(call.getClientId(), call.getRteId(),
		 * call.getTxNum(), null); } else { TPartStoredProcedure sp =
		 * factory.getStoredProcedure( call.getPid(), call.getTxNum());
		 * sp.prepare(call.getPars()); sp.requestConservativeLocks(); spt = new
		 * TPartStoredProcedureTask(call.getClientId(), call.getRteId(),
		 * call.getTxNum(), sp);
		 * 
		 * if (!sp.isReadOnly()) DdRecoveryMgr.logRequest(call); } try {
		 * taskQueue.put(spt); } catch (InterruptedException ex) { if
		 * (logger.isLoggable(Level.SEVERE))
		 * logger.severe("fail to insert task to queue"); }
		 * 
		 * }
		 */
	}

	public void run() {
		long insertedTxNum, lastSunkTxNum = -1;

		while (true) {
			try {
				// blocked if the queue is empty
				StoredProcedureCall call = spcQueue.take();
				TPartStoredProcedureTask task = createStoredProcedureTask(call);

				// Deprecated
				// TPartStoredProcedureTask task = taskQueue.take();

				// schedules the utility procedures directly without T-Part
				// module
				if (task.getProcedureType() == TPartStoredProcedure.POPULATE
						|| task.getProcedureType() == TPartStoredProcedure.PRE_LOAD
						|| task.getProcedureType() == TPartStoredProcedure.PROFILE) {
					lastSunkTxNum = task.getTxNum();
					List<TPartStoredProcedureTask> list = new ArrayList<TPartStoredProcedureTask>();
					list.add(task);

					// Deprecated
					// VanillaDdDb.tpartTaskScheduler().addTask(list.iterator());
					dispatchToTaskMgr(list.iterator());

					continue;
				}

				if (task.getProcedureType() == TPartStoredProcedure.KEY_ACCESS) {

					boolean isCrossPartitionTx = false;
					int lastPart = -1;
					// Check if there are records on two different partitions
					for (RecordKey key : task.getReadSet()) {
						int thisPart = parMetaMgr.getPartition(key);
						if (lastPart != -1 && lastPart != thisPart) {
							isCrossPartitionTx = true;
							break;
						}
						lastPart = thisPart;
					}

					if (isCrossPartitionTx && HAS_REORDERING == 1) {
						remoteTasks.add(task);
					} else {
						Node node = new Node(task);
						inserter.insert(graph, node);
					}

				}

				insertedTxNum = task.getTxNum();
				// sink current t-graph if # pending tx exceeds threshold
				if (insertedTxNum == lastSunkTxNum + NUM_TASK_PER_SINK) {

					lastSunkTxNum = insertedTxNum;
					if (HAS_REORDERING == 1) {
						for (TPartStoredProcedureTask rtask : remoteTasks) {
							Node node = new Node(rtask);
							inserter.insert(graph, node);
						}
					}

					if (graph.getNodes().size() != 0) {
						Iterator<TPartStoredProcedureTask> plansTter = sinker.sink(graph);

						// Deprecated
						// VanillaDdDb.tpartTaskScheduler().addTask(plansTter);
						dispatchToTaskMgr(plansTter);
					}

					if (HAS_REORDERING == 1) {
						remoteTasks.clear();
					}
				}

			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}

	private TPartStoredProcedureTask createStoredProcedureTask(StoredProcedureCall call) {
		if (call.isNoOpStoredProcCall()) {
			return new TPartStoredProcedureTask(call.getClientId(), call.getRteId(), call.getTxNum(), null);
		} else {
			TPartStoredProcedure sp = factory.getStoredProcedure(call.getPid(), call.getTxNum());
			sp.prepare(call.getPars());
			sp.requestConservativeLocks();

			if (!sp.isReadOnly())
				DdRecoveryMgr.logRequest(call);

			return new TPartStoredProcedureTask(call.getClientId(), call.getRteId(), call.getTxNum(), sp);
		}
	}

	private void dispatchToTaskMgr(Iterator<TPartStoredProcedureTask> plans) {
		while (plans.hasNext()) {
			TPartStoredProcedureTask p = plans.next();
			VanillaDb.taskMgr().runTask(p);
		}
	}
}
