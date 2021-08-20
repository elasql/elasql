package org.elasql.schedule.tpart.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;

public class SunkPlan {
	private int sinkProcessId;
	private boolean isHereMaster;

	// key->srcTxNum
	private Map<PrimaryKey, Long> readingInfoMap;

	// destServerId -> PushInfos
	private Map<Integer, Set<PushInfo>> pushingInfoMap;

	private List<PrimaryKey> localWriteBackInfo = new ArrayList<PrimaryKey>();

	// Migration flags
	private Set<PrimaryKey> cacheInsertions = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> cacheDeletions = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> storageInsertions = new HashSet<PrimaryKey>();
	
	// <Record Key -> Target transactions to be passed in local>
	private Map<PrimaryKey, Set<Long>> passToLocalTxns = new HashMap<PrimaryKey, Set<Long>>();

	private Map<Integer, Set<PushInfo>> sinkPushingInfoMap = new HashMap<Integer, Set<PushInfo>>();

	private Set<PrimaryKey> sinkReadingSet = new HashSet<PrimaryKey>();

	// MODIFIED: Add variable to store the Txn node
	private TxNode node;
	private Boolean isRemoteReadPlan;
	private Boolean isRemotePushPlan;

	// MODIFIED: Add variable to pass the TxNode
	public SunkPlan(int sinkProcessId, boolean isHereMaster, TxNode node) {
		this.sinkProcessId = sinkProcessId;
		this.isHereMaster = isHereMaster;
		this.node = node;
		this.isRemoteReadPlan = false;
		this.isRemotePushPlan = false;
	}

	public void addReadingInfo(PrimaryKey key, long srcTxNum) {
		// not need to specify dest, that is the owner tx num
		if (readingInfoMap == null)
			readingInfoMap = new HashMap<PrimaryKey, Long>();
		readingInfoMap.put(key, srcTxNum);

		// MODIFIED: Check whether the plan contains remote read or not.
		if(isRemoteRead(key)){
			isRemoteReadPlan = true;
		}
	}

	public void addPushingInfo(PrimaryKey key, int targetNodeId, long destTxNum) {
		if (pushingInfoMap == null)
			pushingInfoMap = new HashMap<Integer, Set<PushInfo>>();
		Set<PushInfo> pushInfos = pushingInfoMap.get(targetNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			pushingInfoMap.put(targetNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, targetNodeId, key));

		// MODIFIED: Check whether the plan contains push plan or not.
		if(targetNodeId != Elasql.serverId())
			isRemotePushPlan = true;
	}

	public void addLocalPassingTarget(PrimaryKey key, long destTxNum) {
		if (passToLocalTxns.get(key) == null)
			passToLocalTxns.put(key, new HashSet<Long>());
		passToLocalTxns.get(key).add(destTxNum);
	}

	public void addSinkPushingInfo(PrimaryKey key, int destNodeId, long destTxNum) {
		Set<PushInfo> pushInfos = sinkPushingInfoMap.get(destNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			sinkPushingInfoMap.put(destNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, destNodeId, key));

		// MODIFIED: Check whether the plan contains push plan or not.
		if(destNodeId != Elasql.serverId())
			isRemotePushPlan = true;
	}

	public void addSinkReadingInfo(PrimaryKey key) {
		sinkReadingSet.add(key);
	}

	public Map<Integer, Set<PushInfo>> getSinkPushingInfo() {
		return sinkPushingInfoMap;
	}

	public Set<PrimaryKey> getSinkReadingInfo() {
		return sinkReadingSet;
	}

	public Long[] getLocalPassingTarget(PrimaryKey key) {
		Set<Long> set = passToLocalTxns.get(key);
		return (set == null) ? null : set.toArray(new Long[0]);
	}

	public int sinkProcessId() {
		return sinkProcessId;
	}

	public boolean isHereMaster() {
		return isHereMaster;
	}

	public void addLocalWriteBackInfo(PrimaryKey key) {
		localWriteBackInfo.add(key);
	}
	
	public Set<PrimaryKey> getReadSet() {
		if (readingInfoMap == null)
			readingInfoMap = new HashMap<PrimaryKey, Long>();
		return readingInfoMap.keySet();
	}

	public long getReadSrcTxNum(PrimaryKey key) {
		return readingInfoMap.get(key);
	}

	public Map<Integer, Set<PushInfo>> getPushingInfo() {
		return pushingInfoMap;
	}

	public List<PrimaryKey> getLocalWriteBackInfo() {
		return localWriteBackInfo;
	}

	public boolean hasLocalWriteBack() {
		return localWriteBackInfo.size() > 0;
	}

	public boolean hasSinkPush() {
		return sinkPushingInfoMap.size() > 0;
	}

	public void addCacheInsertion(PrimaryKey key) {
		cacheInsertions.add(key);
	}

	public void addCacheDeletion(PrimaryKey key) {
		cacheDeletions.add(key);
	}
	
	public void addStorageInsertion(PrimaryKey key) {
		storageInsertions.add(key);
	}

	public Set<PrimaryKey> getCacheInsertions() {
		return cacheInsertions;
	}

	public Set<PrimaryKey> getCacheDeletions() {
		return cacheDeletions;
	}
	
	public Set<PrimaryKey> getStorageInsertions() {
		return storageInsertions;
	}
	
	public boolean isReadOnly() {
		return localWriteBackInfo.isEmpty();
	}

	// MODIFIED: Add new method to return the set of reading edges
	public Set<Edge> getReadEdges(){
		return this.node.getReadEdges();
	}

	// MODIFIED: Add new method to return the partition ID(the machine ID of the source of the record key)
	public int getSrcParId(PrimaryKey key){
		for(Edge e : this.node.getReadEdges()){
			if(e.getResourceKey() == key){
				return e.getTarget().getPartId();
			}
		}
		return -1;
	}

	// MODIFIED: Add new method to check whether the record is remote read or not
	public Boolean isRemoteRead(PrimaryKey key){
		for(Edge e : this.node.getReadEdges()){
			if(e.getResourceKey() == key){
				if(e.getTarget().getPartId() != Elasql.serverId())
					return true;
				else
					return false;
			}
		}
		return false;
	}

	// MODIFIED: Get the partition ID of the source Txn of specify key.
	public int getPartIdOfSrcTxn(PrimaryKey key){
		for(Edge e : this.node.getReadEdges()){
			if(e.getResourceKey() == key){
				return e.getTarget().getPartId();
			}
		}
		return -1;
	}

	// MODIFIED: Return "isRemoteReadPlan"
	public Boolean isContainRemoteRead(){
		return isRemoteReadPlan;
	}

	// MODIFIED: Return "isRemotePushPlan"
	public Boolean isContainRemotePush(){
		return isRemotePushPlan;
	}
	
	/*
	 * For each tx node, create a procedure task for it. The task will
	 * be scheduled locally if 1) the task is partitioned into current
	 * server or 2) the task needs to write back records to this server.
	 */
	// A plan should be executed in the local node if:
	// - The local node is the master node
	// - It needs to write back records to the local storage (sink)
	// - It needs to push data from the local storage to remote
	// - It needs to delete cached records (Hermes-specific)
	public boolean shouldExecuteHere() {
		return isHereMaster || hasLocalWriteBack() || hasSinkPush() ||
				!cacheDeletions.isEmpty();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("Sink Process Id: ");
		sb.append(sinkProcessId);
		sb.append("\n");

		sb.append("Is Local: ");
		sb.append(isHereMaster);
		sb.append("\n");

		sb.append("Reading Info: ");
		sb.append(readingInfoMap);
		sb.append("\n");

		sb.append("Pushing Info: ");
		sb.append(pushingInfoMap);
		sb.append("\n");

		sb.append("Local Writing Back Info: ");
		sb.append(localWriteBackInfo);
		sb.append("\n");

		sb.append("Write Dest: ");
		Iterator<?> iterator = null;
		if (passToLocalTxns != null) {
			iterator = passToLocalTxns.keySet().iterator();

			while (iterator.hasNext()) {
				PrimaryKey key = (PrimaryKey) iterator.next();
				Set<Long> value = passToLocalTxns.get(key);
				sb.append(key + " : [");
				for (Long p : value)
					sb.append(p + ",");
				sb.append("]");
			}
		}

		sb.append("\n");

		sb.append("Sink Pushing Info: ");
		if (sinkPushingInfoMap != null) {
			iterator = sinkPushingInfoMap.keySet().iterator();
			while (iterator.hasNext()) {
				Integer key = (Integer) iterator.next();
				Set<PushInfo> value = sinkPushingInfoMap.get(key);
				sb.append(key + " : [");
				for (PushInfo p : value)
					sb.append(p + ",");
				sb.append("]");

			}
		}
		sb.append("\n");

		sb.append("Sink Reading Info: ");
		if (sinkReadingSet != null) {
			iterator = sinkReadingSet.iterator();
			while (iterator.hasNext()) {
				sb.append(iterator.next() + ",");

			}
		}

		sb.append("\n");
		
		sb.append("Cache Insertions: ");
		sb.append(cacheInsertions);
		sb.append("\n");
		
		sb.append("Cache Deletions: ");
		sb.append(cacheDeletions);
		sb.append("\n");
		
		sb.append("Storage Deletions: ");
		sb.append(storageInsertions);
		sb.append("\n");

		return sb.toString();
	}
}