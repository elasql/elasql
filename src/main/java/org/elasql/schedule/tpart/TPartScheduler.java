package org.elasql.schedule.tpart;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.Scheduler;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.sink.Sinker;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartScheduler extends Task implements Scheduler {
	private static Logger logger = Logger.getLogger(TPartScheduler.class.getName());

	public static final int SCHEDULE_BATCH_SIZE;

	private TPartStoredProcedureFactory factory;
	
//	private File dumpDir = new File("batch_dump");

	static {
		SCHEDULE_BATCH_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartScheduler.class.getName() + ".SCHEDULE_BATCH_SIZE", 10);
	}

	private BlockingQueue<StoredProcedureCall> spcQueue;
	private BatchNodeInserter inserter;
	private Sinker sinker;
	private TGraph graph;

	public TPartScheduler(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, Sinker sinker, TGraph graph) {
		this.factory = factory;
		this.inserter = inserter;
		this.sinker = sinker;
		this.graph = graph;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		// Clear the dump dir
//		dumpDir.mkdirs();
//		for (File file : dumpDir.listFiles()) {
//			if (file.isFile())
//				file.delete();
//		}
	}

	public void schedule(StoredProcedureCall call) {
		try {
			spcQueue.put(call);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		List<TPartStoredProcedureTask> batchedTasks = new LinkedList<TPartStoredProcedureTask>();
		
		Thread.currentThread().setName("T-Part Scheduler");
		
		while (true) {
			try {
				// blocked if the queue is empty
				StoredProcedureCall call = spcQueue.take();

				TransactionProfiler.setProfiler(call.getProfiler());
				TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();

				profiler.stopComponentProfiler("OU0 - Dispatch to router");
				profiler.startComponentProfiler("OU0 - ROUTE");
				
				TPartStoredProcedureTask task = createStoredProcedureTask(call, profiler);

				// schedules the utility procedures directly without T-Part
				// module
				if (task.getProcedureType() == ProcedureType.UTILITY) {
//					VanillaDb.taskMgr().runTask(task);
					continue;
				}

				// TODO: Uncomment this when the migration module is migrated
//				if (task.getProcedureType() == ProcedureType.MIGRATION) {
//					// Process and dispatch it immediately
//					processMigrationTx(task);
//					continue;
//				}

				if (task.getProcedureType() == ProcedureType.NORMAL) {
					batchedTasks.add(task);
				}

				// sink current t-graph if # pending tx exceeds threshold
				if (!inserter.needBatching() || batchedTasks.size() >= SCHEDULE_BATCH_SIZE) {
					processBatch(batchedTasks);
					batchedTasks.clear();
				}

			} catch (InterruptedException | IOException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}
	
	private void processBatch(List<TPartStoredProcedureTask> batchedTasks) {
		// Insert the batch of tasks
		inserter.insertBatch(graph, batchedTasks);	

		// Debug
//		printGraphStatistics();
//		System.out.println(graph);
//		printStatistics();
//		printImbalStatistics();
//		printTargetPartitionDist();
//		collectGraphStatistics();
		
		// Sink the graph
		if (graph.getTxNodes().size() != 0) {
			// Record plan gen start time, CPU start time, disk IO count
			for(TPartStoredProcedureTask task : batchedTasks) {
				TransactionProfiler profiler = task.getTxProfiler();
				profiler.stopComponentProfiler("OU0 - ROUTE");
				profiler.startComponentProfiler("OU1 - Generate Plan");
			}
			
			Iterator<TPartStoredProcedureTask> plansTter = sinker.sink(graph);

			// Record plan gen stop time, CPU stop time, disk IO count
			for(TPartStoredProcedureTask task : batchedTasks)
				task.getTxProfiler().stopComponentProfiler("OU1 - Generate Plan");
			// Record thread init start time, CPU start time, disk IO count
			for(TPartStoredProcedureTask task : batchedTasks)
				task.getTxProfiler().startComponentProfiler("OU2 - Initialize Thread");
			
			dispatchToTaskMgr(plansTter);
		}
	}
	
	// TODO: Uncomment this when the migration module is migrated
//	private void processMigrationTx(TPartStoredProcedureTask task) {
//		// Insert the task to T-Graph
//		ColdMigrationProcedure sp = (ColdMigrationProcedure) task.getProcedure();
//		graph.insertTxNode(task, sp.getMigrationRange().getDestPartId());
//		
//		// Sink the graph
//		Iterator<TPartStoredProcedureTask> plansTter = sinker.sink(graph);
//		dispatchToTaskMgr(plansTter);
//	}

	private TPartStoredProcedureTask createStoredProcedureTask(StoredProcedureCall call, TransactionProfiler profiler)
			throws IOException {
		if (call.isNoOpStoredProcCall()) {
			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(),
					call.getTxNum(), call.getArrivedTime(), profiler, null, null,
					call.getRoute());
		} else {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(call.getPid(), call.getTxNum());
			sp.prepare(call.getPars());

			if (!sp.isReadOnly())
				DdRecoveryMgr.logRequest(call);
			
			// Take out the transaction estimation
			TransactionEstimation estimation = null;
			if (call.getMetadata() != null) {
				if (call.getMetadata().getClass().equals(byte[].class)) {
					byte[] data = (byte[]) call.getMetadata();
					estimation = TransactionEstimation.fromBytes(data);
				}
			}

			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(),
					call.getTxNum(), call.getArrivedTime(), profiler, sp, estimation, call.getRoute());
		}
	}

	private void dispatchToTaskMgr(Iterator<TPartStoredProcedureTask> plans) {
		while (plans.hasNext()) {
			TPartStoredProcedureTask p = plans.next();
			VanillaDb.taskMgr().runTask(p);
		}
	}
	
	private int batchId = 0;
	private int imbalanced = 0;
	private int remoteTxRead = 0;
	private int remoteSinkRead = 0;
	private int recordCount = 0;
	private long nextReportTime = 0;
	
	private void printGraphStatistics() {
		// XXX: Show the statistics of the T-Graph
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
//		if (batchId % 100 == 0) {
//			String stat = graph.getStatistics();
//			System.out.println("Time: " + time);
//			System.out.println("T-Graph id: " + (batchId + 1));
//			System.out.print(stat);
			
			imbalanced += graph.getImbalancedDis();
			remoteTxRead += graph.getRemoteTxReads();
			remoteSinkRead += graph.getRemoteSinkReads();
			recordCount++;
			
			if (time >= nextReportTime) {
//				System.out.println("======== Total Statistics ========");
				System.out.println(String.format("Time: %d, avg. imbal: %f, avg. remote tx reads: %f, "
						+ "avg. remote sink reads: %f", time, ((double) imbalanced) / recordCount,
						((double) remoteTxRead) / recordCount, ((double) remoteSinkRead) / recordCount));
//				System.out.println("==================================\n");
				
				imbalanced = 0;
				remoteTxRead = 0;
				remoteSinkRead = 0;
				recordCount = 0;
				nextReportTime = time + 5;
				
				// Dump the current graph
//				GraphDumper.dumpToFile(new File(dumpDir, String.format("%d_%d.txt", time, batchId)), graph);
			}
//		}
		batchId++;
	}
	
	private BufferedWriter writer = null;
	
	private void printTargetPartitionDist() {
		// Calculate the distributions
		int[] dists = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (TxNode node : graph.getTxNodes()) {
			for (Edge e : node.getReadEdges()) {
				PrimaryKey key = e.getResourceKey();
				if (key.getTableName().equals("warehouse")) {
					int wid = (Integer) key.getVal(0).asJavaVal();
					int partId = (wid - 1) / 20;
					dists[partId]++;
				}	
			}
		}
		
		// Write to a file
		try {
			if (writer == null)
				writer = new BufferedWriter(new FileWriter("part-dist.txt"));
			
			writer.write(Arrays.toString(dists));
			writer.newLine();
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private int[] numberOfTxns = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int[] numberOfDistTxns = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private double[] ratioOfRemoteReads = new double[PartitionMetaMgr.NUM_PARTITIONS];
	
	private void collectGraphStatistics() {
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
		
		// collects
		for (TxNode node : graph.getTxNodes()) {
			int masterId = node.getPartId();
			boolean distTx = false;
			
			// count remote reads
			int remoteReads = 0;
			for (Edge e : node.getReadEdges()) {
				int resPartId = e.getTarget().getPartId();
				if (masterId != resPartId) {
					remoteReads++;
					distTx = true;
				}
			}
			double remoteReadRatio = ((double) remoteReads) / node.getReadEdges().size();
			ratioOfRemoteReads[masterId] += remoteReadRatio;
			
			// count transactions
			numberOfTxns[masterId]++;
			if (distTx) {
				numberOfDistTxns[masterId]++;
			}
		}
		
		// prints every period of time
		if (time >= nextReportTime) {
			System.out.println(String.format("Time: %d", time));
			
			// # of transactions
			System.out.println(String.format("Number of transactions: %s.",
					Arrays.toString(numberOfTxns)));
			
			// % of dist transactions
			double[] ratioOfDistTxns = new double[PartitionMetaMgr.NUM_PARTITIONS];
			for (int i = 0; i < ratioOfDistTxns.length; i++) {
				if (numberOfTxns[i] == 0) {
					ratioOfDistTxns[i] = 0.0;
				} else {
					ratioOfDistTxns[i] = numberOfDistTxns[i];
					ratioOfDistTxns[i] /= numberOfTxns[i];
				}
			}
			System.out.println(String.format("Ratio of dist. transactions: %s.",
					Arrays.toString(ratioOfDistTxns)));
			
			// # of remote reads
			for (int i = 0; i < ratioOfRemoteReads.length; i++) {
				if (numberOfTxns[i] == 0) {
					ratioOfRemoteReads[i] = 0.0;
				} else {
					ratioOfRemoteReads[i] /= numberOfTxns[i];
				}
			}
			System.out.println(String.format("Average Remote Read Ratio: %s.",
					Arrays.toString(ratioOfRemoteReads)));
			
			// Reset all numbers
			Arrays.fill(numberOfTxns, 0);
			Arrays.fill(numberOfDistTxns, 0);
			Arrays.fill(ratioOfRemoteReads, 0.0);
			
			nextReportTime = time + 5;
		}
	}
	
	private int totalImbalanced = 0;
	private int numOfBatches = 0;
	
	private void printImbalStatistics() {
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
		
		numOfBatches++;
		totalImbalanced += graph.getImbalancedDis();
		
		if (time >= nextReportTime) {
			double averageImb = totalImbalanced;
			averageImb /= numOfBatches;
			System.out.println(String.format("Average imbalanced: %f at time %d.",
					averageImb, time));
			
			numOfBatches = 0;
			totalImbalanced = 0;
			nextReportTime = time + 3;
		}
	}
}
