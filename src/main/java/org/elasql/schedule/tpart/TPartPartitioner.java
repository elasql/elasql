package org.elasql.schedule.tpart;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.Scheduler;
import org.elasql.schedule.tpart.sink.Sinker;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class TPartPartitioner extends Task implements Scheduler {

	public static CostFunctionCalculator costFuncCal;

	private static Logger logger = Logger.getLogger(TPartPartitioner.class.getName());

	private static final Class<?> COST_FUNC_CLASS;

	private static final int NUM_TASK_PER_SINK;

	private static final int HAS_REORDERING;

	private List<TPartStoredProcedureTask> remoteTasks = new LinkedList<TPartStoredProcedureTask>();

	private TPartStoredProcedureFactory factory;

	private PartitionMetaMgr parMetaMgr = Elasql.partitionMetaMgr();

	static {
		HAS_REORDERING = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPartitioner.class.getName() + ".HAS_REORDERING", 0);

		COST_FUNC_CLASS = ElasqlProperties.getLoader().getPropertyAsClass(
				TPartPartitioner.class.getName() + ".COST_FUNC_CLASS", null, CostFunctionCalculator.class);
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

	public TPartPartitioner(TPartStoredProcedureFactory factory, 
			NodeInserter inserter, Sinker sinker, TGraph graph) {
		this.factory = factory;
		this.inserter = inserter;
		this.sinker = sinker;
		this.graph = graph;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
	}

	public void schedule(StoredProcedureCall... calls) {
		for (StoredProcedureCall call : calls)
			spcQueue.add(call);
	}
	
	private int id = 0;

	public void run() {
		long insertedTxNum, lastSunkTxNum = -1;

		while (true) {
			try {
				// blocked if the queue is empty
				StoredProcedureCall call = spcQueue.take();
				TPartStoredProcedureTask task = createStoredProcedureTask(call);

				// schedules the utility procedures directly without T-Part
				// module
				if (task.getProcedureType() == ProcedureType.UTILITY) {
					lastSunkTxNum = task.getTxNum();
					List<TPartStoredProcedureTask> list = new ArrayList<TPartStoredProcedureTask>();
					list.add(task);

					dispatchToTaskMgr(list.iterator());

					continue;
				}

				if (task.getProcedureType() == ProcedureType.NORMAL) {

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
					
					// XXX: Show the statistics of the T-Graph
//					long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
//					if (id % 100 == 0) {
//						String stat = graph.getStatistics();
//						System.out.println("Time: " + time + ", T-Graph id: " + (id + 1));
//						System.out.println(stat);
//					}
//					id++;

					if (graph.getNodes().size() != 0) {
						Iterator<TPartStoredProcedureTask> plansTter = sinker.sink(graph);

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
			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(), call.getTxNum(), null);
		} else {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(call.getPid(), call.getTxNum());
			sp.prepare(call.getPars());

			if (!sp.isReadOnly())
				DdRecoveryMgr.logRequest(call);

			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(), call.getTxNum(), sp);
		}
	}

	private void dispatchToTaskMgr(Iterator<TPartStoredProcedureTask> plans) {
		while (plans.hasNext()) {
			TPartStoredProcedureTask p = plans.next();
			VanillaDb.taskMgr().runTask(p);
		}
	}
}
