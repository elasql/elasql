package org.elasql.server.migration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.remote.groupcomm.server.ConnectionMgr;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public abstract class MigrationManager {
	private static Logger logger = Logger.getLogger(MigrationManager.class.getName());

	// Sink ids for sequencers to identify the messages of migration
	public static final int SINK_ID_START_MIGRATION = -555;
	public static final int SINK_ID_ANALYSIS = -777;
	public static final int SINK_ID_ASYNC_PUSHING = -888;
	public static final int SINK_ID_STOP_MIGRATION = -999;

	public static long startTime = System.currentTimeMillis();

	private AtomicBoolean isMigrating = new AtomicBoolean(false);
	private AtomicBoolean isMigrated = new AtomicBoolean(false);

	// For recording the migrated status in the other node and the destination
	// node
	private Set<RecordKey> migratedKeys = new HashSet<RecordKey>(1000000);
	// These two sets are created for the source node identifying the migrated
	// records
	// and the parameters of background pushes.
	private Map<RecordKey, Boolean> newInsertedData = new HashMap<RecordKey, Boolean>(1000000);
	private Map<RecordKey, Boolean> analyzedData;

	private AtomicBoolean analysisStarted = new AtomicBoolean(false);
	private AtomicBoolean analysisCompleted = new AtomicBoolean(false);

	// Async pushing
	private static int PUSHING_COUNT = 5000;
	private static final int PUSHING_BYTE_COUNT = 4000000;
	private ConcurrentLinkedQueue<RecordKey> skipRequestQueue = new ConcurrentLinkedQueue<RecordKey>();
	private Map<String, Set<RecordKey>> bgPushCandidates = new HashMap<String, Set<RecordKey>>();
	private boolean roundrobin = true;
	private boolean useCount = true;
	private boolean backPushStarted = false;
	private boolean isSourceNode;

	// Clay structure
	private int sourceNode, destNode;
	private boolean isSeqNode;
	public static final int MONITORING_TIME = 30 * 1000;
	public static int CLAY_EPOCH = 0;
	private final int LOOK_AHEAD = 200;
	public static int dataRange = 100;
	public static double BETA = 0.5;
	private static HashMap<Integer, Vertex> vertexKeys = new HashMap<Integer, Vertex>(1000000);
	protected HashSet<Integer> migrateRanges = new HashSet<Integer>();

	// The time starts from the time which the first transaction arrives at
	private long printStatusPeriod;

	public static long MONITOR_STOP_TIME = -1;
	public static AtomicBoolean isMonitoring = new AtomicBoolean(false);

	public MigrationManager(long printStatusPeriod) {
		this.sourceNode = 0;
		this.destNode = 1;
		this.printStatusPeriod = printStatusPeriod;
		isSourceNode = (Elasql.serverId() == getSourcePartition());
		isSeqNode = (Elasql.serverId() == ConnectionMgr.SEQ_NODE_ID);
	}

	public void encreaseWeight(Integer vetxId, int partId) {

		if (!vertexKeys.containsKey(vetxId))
			vertexKeys.put(vetxId, new Vertex(vetxId, partId));
		else
			vertexKeys.get(vetxId).add();
	}

	public void encreaseWeight(RecordKey k, int partId) {
		Integer vetxId = Integer.parseInt(k.getKeyVal("i_id").toString()) / dataRange;
		if (!vertexKeys.containsKey(vetxId))
			vertexKeys.put(vetxId, new Vertex(vetxId, partId));
		else
			vertexKeys.get(vetxId).add();
	}

	public void encreaseEdge(List<Integer> vertexIdSet) {
		for (int i : vertexIdSet)
			for (int j : vertexIdSet)
				if (j != i)
					vertexKeys.get(i).addEdge(j, vertexKeys.get(j).getPartId());
	}

	public HashMap<Integer, Vertex> getVertexKeys() {
		return vertexKeys;
	}
	
	public void cleanUpClay(){
		for(Vertex v : vertexKeys.values())
			v.clear();
		vertexKeys.clear();
	}

	public void startClayMonitoring() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Clay Start Monitoring at " + (System.currentTimeMillis() - startTime) / 1000);

		isMonitoring.set(true);
		MONITOR_STOP_TIME = System.currentTimeMillis() + MONITORING_TIME;

	}

	public void generateMigrationPlan() {

		// System.out.println(VertexKeys);

		VanillaDb.taskMgr().runTask(new Task() {

			@Override
			public void run() {

				long start_t = System.currentTimeMillis();
				ArrayList<Partition> partitions = new ArrayList<Partition>();
				for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
					partitions.add(new Partition(i));

				for (Vertex e : vertexKeys.values())
					partitions.get(e.getPartId()).addVertex(e);

				System.out.println("Clay No." + CLAY_EPOCH + " Monitoring Finish!");
				for (Partition p : partitions) {
					System.out
							.println("Part : " + p.getId() + " Weight : " + p.getLoad() + " Edge : " + p.getEdgeLoad());

					for (Vertex v : p.getFirstTen())
						System.out.println("Top Ten : " + v.getId() + " PartId : " + v.getPartId() + " Weight :"
								+ v.getVertexWeight());
				}

				double avgLoad = 0;
				for (Partition p : partitions)
					avgLoad += p.getTotalLoad();
				avgLoad /= PartitionMetaMgr.NUM_PARTITIONS;

				LinkedList<Partition> overloadParts = new LinkedList<Partition>();
				for (Partition p : partitions)
					if (p.getTotalLoad() > avgLoad)
						overloadParts.add(p);

				Collections.sort(overloadParts);
				// Get Most Overloaded Partition
				Partition overloadPart = overloadParts.getLast();

				Candidate migraCandidate = new Candidate();
				// Init Clump with most Hotest Vertex
				migraCandidate.addCandidate(overloadPart.getHotestVertex());

				// Expend LOOK_AHEAD times
				for (int a = 0; a < LOOK_AHEAD; a++)
					migraCandidate.addCandidate(vertexKeys.get(migraCandidate.getHotestNeighbor()));

				// System.out.println(migraCandidate);
				// Preparse Param
				LinkedList<Integer> params = new LinkedList<Integer>(migraCandidate.getCandidateIds());

				// Determinstic select last load partition as Dest
				Collections.sort(partitions);
				params.addFirst(new Integer(partitions.get(0).getId()));

				// Add Source at the Source
				params.addFirst(new Integer(overloadPart.getId()));

				System.out.println("Clay No." + CLAY_EPOCH + " Migration Plan Generation Takes : "
						+ (System.currentTimeMillis() - start_t) + " ms");

				System.out.println("Source is Part : " + overloadPart.getId() + " Weight : " + overloadPart.getLoad()
						+ " Edge : " + overloadPart.getEdgeLoad());
				System.out.println("Dest is Part : " + partitions.get(0).getId() + " Weight : "
						+ partitions.get(0).getLoad() + " Edge : " + partitions.get(0).getEdgeLoad());
				ArrayList<Integer> aa = new ArrayList<Integer>(migraCandidate.getCandidateIds());
				Collections.sort(aa);
				System.out.println(aa);

				CLAY_EPOCH = CLAY_EPOCH + 1;
				if (logger.isLoggable(Level.INFO))
					logger.info("Clay BroadKeyscast at " + (System.currentTimeMillis() - startTime) / 1000);

				broadcastMigrateKeys(params.toArray(new Integer[0]));

			}
		});

	}

	// public static void main(String[] arg) {
	//
	// System.out.println("df");
	// int partId;
	//
	// HashMap<String, Constant> tmp = new HashMap<String, Constant>();
	// tmp.put("i_id", new IntegerConstant(1));
	// RecordKey tmpK = new RecordKey("item", tmp);
	// partId = 0;
	// encreaseWeight(tmpK, partId);
	//
	// tmp = new HashMap<String, Constant>();
	// tmp.put("i_id", new IntegerConstant(1));
	// tmpK = new RecordKey("item", tmp);
	// partId = 0;
	// encreaseWeight(tmpK, partId);
	//
	// tmp = new HashMap<String, Constant>();
	// tmp.put("i_id", new IntegerConstant(101));
	// tmpK = new RecordKey("item", tmp);
	// partId = 0;
	// encreaseWeight(tmpK, partId);
	//
	// tmp = new HashMap<String, Constant>();
	// tmp.put("i_id", new IntegerConstant(201));
	// tmpK = new RecordKey("item", tmp);
	// partId = 0;
	// encreaseWeight(tmpK, partId);
	//
	// tmp = new HashMap<String, Constant>();
	// tmp.put("i_id", new IntegerConstant(301));
	// tmpK = new RecordKey("item", tmp);
	// partId = 0;
	// encreaseWeight(tmpK, partId);
	//
	// LinkedList<Integer> s = new LinkedList<Integer>();
	// s.add(0);
	// s.add(1);
	// s.add(3);
	// encreaseEdge(s);
	// encreaseEdge(s);
	// System.out.println(vertexKeys);
	//
	// }

	public void addMigrationRanges(Integer[] integers) {
		// Clear previous migration range
		migrateRanges.clear();
		for (int i : integers)
			this.migrateRanges.add(i);
	}

	public abstract boolean keyIsInMigrationRange(RecordKey key);

	public abstract void onReceieveLaunchClayReq(Object[] metadata);

	public abstract void broadcastMigrateKeys(Object[] metadata);

	public abstract void onReceiveStartMigrationReq(Object[] metadata);

	public abstract void onReceiveAnalysisReq(Object[] metadata);

	public abstract void onReceiveAsyncMigrateReq(Object[] metadata);

	public abstract void onReceiveStopMigrateReq(Object[] metadata);

	public abstract void prepareAnalysis();

	public abstract int recordSize(String tableName);

	public abstract Map<RecordKey, Boolean> generateDataSetForMigration();

	// XXX: Assume there are 3 server nodes
	// The better way to set up is to base on NUM_PARTITIONS
	// , but this version doesn't have properties loader.
	// It may cause NUM_PARTITIONS too early to be read.
	// That is, NUM_PARTITIONS is read before the properties loaded.

	public int getSourcePartition() {
		return sourceNode;
		// return NUM_PARTITIONS - 2;
	}

	public void setSourcePartition(int id) {
		this.sourceNode = id;
	}

	public void setDestPartition(int id) {
		this.destNode = id;
	}

	public int getDestPartition() {
		return destNode;
		// return NUM_PARTITIONS - 1;
	}

	public int getMonitorNode() {
		return 2;
	}

	// Executed on the source node
	public void analysisComplete(Map<RecordKey, Boolean> analyzedKeys) {
		// Only the source node can call this method
		if (!isSourceNode)
			throw new RuntimeException("Something wrong");

		// Set all keys in the data set as pushing candidates
		// asyncPushingCandidates = new HashSet<RecordKey>(newDataSet.keySet());

		for (RecordKey rk : analyzedKeys.keySet())
			addBgPushKey(rk);

		// Save the set
		analyzedData = analyzedKeys;

		System.out.println("End of analysis: " + (System.currentTimeMillis() - startTime) / 1000);
	}

	// NOTE: This can only be called by the scheduler
	public void setRecordMigrated(Collection<RecordKey> keys) {
		if (isSourceNode) {
			for (RecordKey key : keys) {
				Boolean status = newInsertedData.get(key);
				if (status != null && status == Boolean.FALSE) {
					skipRequestQueue.add(key);
					newInsertedData.put(key, Boolean.TRUE);
				} else {
					status = analyzedData.get(key);
					if (status != null && status == Boolean.FALSE) {
						skipRequestQueue.add(key);
						analyzedData.put(key, Boolean.TRUE);
					}
				}
			}
		} else {
			migratedKeys.addAll(keys);
		}
	}

	public void setRecordLocation(Collection<RecordKey> keys) {
		if (isSourceNode) {
			for (RecordKey key : keys) {
				Boolean status = newInsertedData.get(key);
				if (status != null && status == Boolean.FALSE) {
					skipRequestQueue.add(key);
					newInsertedData.put(key, Boolean.TRUE);
				} else {
					status = analyzedData.get(key);
					if (status != null && status == Boolean.FALSE) {
						skipRequestQueue.add(key);
						analyzedData.put(key, Boolean.TRUE);
					}
				}
				Elasql.partitionMetaMgr().setPartition(key, destNode);
			}
		} else {
			for (RecordKey key : keys)
				Elasql.partitionMetaMgr().setPartition(key, destNode);
		}

	}

	// NOTE: This can only be called by the scheduler
	public boolean isRecordMigrated(RecordKey key) {
		if (isSourceNode) {
			Boolean status = newInsertedData.get(key);
			if (status != null)
				return status;

			status = analyzedData.get(key);
			if (status != null)
				return status;

			// If there is no candidate in the map, it means that the record
			// must not be inserted before the migration starts. Therefore,
			// the record must have been foreground pushed.
			return true;
		} else {
			if (Elasql.partitionMetaMgr().getPartition(key) != sourceNode) {
				if (Elasql.partitionMetaMgr().getPartition(key) != destNode)
					System.out.println("Something wrong : " + key + " Not at Dest");
				return true;
			} else
				return false;

		}
	}

	// NOTE: only for the normal transactions on the source node
	public void addNewInsertKey(RecordKey key) {
		newInsertedData.put(key, Boolean.FALSE);
		addBgPushKey(key);
	}

	// Note that there may be duplicate keys
	private synchronized void addBgPushKey(RecordKey key) {
		Set<RecordKey> set = bgPushCandidates.get(key.getTableName());
		if (set == null)
			set = new HashSet<RecordKey>();
		bgPushCandidates.put(key.getTableName(), set);
		set.add(key);
	}

	public void startAnalysis() {
		analysisStarted.set(true);
		analysisCompleted.set(false);
	}

	public void startMigration() {
		analysisCompleted.set(true);
		isMigrating.set(true);
		isMigrated.set(false);
		if (logger.isLoggable(Level.INFO))
			logger.info("Migration starts at " + (System.currentTimeMillis() - startTime) / 1000);

		// Start background pushes immediately
		startBackgroundPush();
	}

	public void stopMigration() {
		isMigrating.set(false);
		isMigrated.set(true);
		backPushStarted = false;
		if (migratedKeys != null)
			migratedKeys.clear();
		if (newInsertedData != null)
			newInsertedData.clear();
		if (analyzedData != null)
			analyzedData.clear();
		if (logger.isLoggable(Level.INFO))
			logger.info("Migration completes at " + (System.currentTimeMillis() - startTime) / 1000);
	}

	public boolean isAnalyzing() {
		return analysisStarted.get() && !analysisCompleted.get();
	}

	public boolean isMigrating() {
		return isMigrating.get();
	}

	public boolean isMigrated() {
		return isMigrated.get();
	}

	// Only works on the source node
	protected synchronized RecordKey[] getAsyncPushingParameters() {
		Set<RecordKey> pushingKeys = new HashSet<RecordKey>();
		Set<String> emptyTables = new HashSet<String>();
		int pushing_bytes = 0;

		// YS version
		// for (RecordKey key : asyncPushingCandidates) {
		// seenSet.add(key);
		// if (!isRecordMigrated(key))
		// pushingSet.add(key);
		//
		// if (pushingSet.size() >= PUSHING_COUNT || seenSet.size() >=
		// asyncPushingCandidates.size())
		// break;
		// }
		//
		// // Remove seen records
		// asyncPushingCandidates.removeAll(seenSet);
		//
		// if (logger.isLoggable(Level.INFO))
		// logger.info("The rest size of candidates: " +
		// asyncPushingCandidates.size());

		// Remove the records that have been sent during fore-ground pushing
		RecordKey sentKey = null;
		while ((sentKey = skipRequestQueue.poll()) != null) {
			Set<RecordKey> keys = bgPushCandidates.get(sentKey.getTableName());
			if (keys != null)
				keys.remove(sentKey);
		}

		if (roundrobin) {
			if (useCount) {
				// RR & count
				while (pushingKeys.size() < PUSHING_COUNT && !bgPushCandidates.isEmpty()) {
					for (String tableName : bgPushCandidates.keySet()) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							continue;
						}
						RecordKey key = set.iterator().next();
						pushingKeys.add(key);
						set.remove(key);
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
					}
				}
			} else {
				// RR & byte
				while (pushing_bytes < PUSHING_BYTE_COUNT && !bgPushCandidates.isEmpty()) {
					for (String tableName : bgPushCandidates.keySet()) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							continue;
						}
						RecordKey key = set.iterator().next();
						pushingKeys.add(key);
						set.remove(key);
						pushing_bytes += recordSize(tableName);
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
					}
				}
			}
		} else {
			List<String> candidateTables = new LinkedList<String>(bgPushCandidates.keySet());
			// sort by table size , small first
			Collections.sort(candidateTables, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					int s1 = bgPushCandidates.get(o1).size();
					int s2 = bgPushCandidates.get(o2).size();
					if (s1 > s2)
						return -1;
					if (s1 < s2)
						return 1;
					return 0;
				}
			});
			System.out.println("PUSHING_BYTE_COUNT" + PUSHING_BYTE_COUNT);
			if (useCount) {
				while (!bgPushCandidates.isEmpty() && pushingKeys.size() < PUSHING_COUNT) {
					for (String tableName : candidateTables) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						while (!set.isEmpty() && pushingKeys.size() < PUSHING_COUNT) {
							RecordKey key = set.iterator().next();
							pushingKeys.add(key);
							set.remove(key);
						}

						if (set.isEmpty()) {
							emptyTables.add(tableName);
						} else {
							break;
						}
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
						candidateTables.remove(table);
					}
				}

			} else {
				while (!bgPushCandidates.isEmpty() && pushing_bytes < PUSHING_BYTE_COUNT) {
					for (String tableName : candidateTables) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						while (!set.isEmpty() && pushing_bytes < PUSHING_BYTE_COUNT) {
							RecordKey key = set.iterator().next();
							pushingKeys.add(key);
							set.remove(key);
							pushing_bytes += recordSize(tableName);
						}

						System.out.println("listEmpty: " + set.isEmpty());
						System.out.println("pushing_bytes: " + pushing_bytes);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							// if (!pushingSet.isEmpty()) {
							// VanillaDdDb.taskMgr().runTask(new Task() {
							//
							// @Override
							// public void run() {
							//
							// VanillaDdDb.initAndStartProfiler();
							// try {
							// Thread.sleep(10000);
							// } catch (InterruptedException e) {
							// // TODO Auto-generated catch block
							// e.printStackTrace();
							// }
							// VanillaDdDb.stopProfilerAndReport();
							// }
							//
							// });
							// }
							break;
						} else {
							break;
						}
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
						candidateTables.remove(table);
					}
					System.out.println("pushingSet: " + pushingKeys.size());
					if (!pushingKeys.isEmpty()) {
						break;
					}
				}
			}

		}

		/*
		 * StringBuilder sb = new StringBuilder(); for (String tableName :
		 * roundRobinAsyncPushingCandidates.keySet()) { sb.append(tableName);
		 * sb.append(": ");
		 * sb.append(roundRobinAsyncPushingCandidates.get(tableName).size());
		 * sb.append("\n"); } for (List<RecordKey> table :
		 * roundRobinAsyncPushingCandidates.values()) { candidatesLeft +=
		 * table.size(); }
		 * 
		 * if (logger.isLoggable(Level.INFO)) logger.info(
		 * "The rest size of candidates: " + candidatesLeft + "\n" +
		 * sb.toString());
		 */

		return pushingKeys.toArray(new RecordKey[0]);
	}

	private void startBackgroundPush() {
		if (backPushStarted)
			return;

		backPushStarted = true;

		// Only the source node can send the bg push request
		if (isSourceNode) {
			// Use another thread to start background pushing
			VanillaDb.taskMgr().runTask(new Task() {
				@Override
				public void run() {
					TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ASYNC_PUSHING);

					Elasql.connectionMgr().pushTupleSet(Elasql.migrationMgr().getSourcePartition(), ts);

					if (logger.isLoggable(Level.INFO))
						logger.info("Trigger background pushing");
					/*
					 * long tCount = (System.currentTimeMillis() -
					 * CalvinStoredProcedureTask.txStartTime) /
					 * printStatusPeriod; StringBuilder sb = new
					 * StringBuilder(); String preStr = "";
					 * 
					 * for (String tableName : bgPushCandidates.keySet()) {
					 * Set<RecordKey> keys = bgPushCandidates.get(tableName); if
					 * (keys != null) { sb.append(tableName); sb.append(":");
					 * sb.append(keys.size()); sb.append(","); } } preStr =
					 * "\nTable remain|" + sb.toString() + "\n"; sb = new
					 * StringBuilder(); for (long i = 0; i < tCount - 1; i++)
					 * sb.append(preStr); if (logger.isLoggable(Level.INFO))
					 * logger.info(sb.toString()); while (true) { sb = new
					 * StringBuilder(); for (String tableName :
					 * bgPushCandidates.keySet()) { Set<RecordKey> keys =
					 * bgPushCandidates.get(tableName); if (keys != null) {
					 * sb.append(tableName); sb.append(":");
					 * sb.append(keys.size()); sb.append(","); } }
					 * 
					 * if (logger.isLoggable(Level.INFO)) logger.info(
					 * "\nTable remain|" + sb.toString()); try {
					 * Thread.sleep(printStatusPeriod); } catch
					 * (InterruptedException e) { e.printStackTrace(); } }
					 */
				}
			});
		}
	}

}