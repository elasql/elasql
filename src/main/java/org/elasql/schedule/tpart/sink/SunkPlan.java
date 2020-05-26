package org.elasql.schedule.tpart.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.RecordKey;

public class SunkPlan {
	private int sinkProcessId;
	private boolean isHereMaster;

	// key->srcTxNum
	private Map<RecordKey, Long> readingInfoMap;

	// destServerId -> PushInfos
	private Map<Integer, Set<PushInfo>> pushingInfoMap;

	private List<RecordKey> localWriteBackInfo = new ArrayList<RecordKey>();

	// Migration flags
	private Set<RecordKey> cacheInsertions = new HashSet<RecordKey>();
	private Set<RecordKey> cacheDeletions = new HashSet<RecordKey>();
	private Set<RecordKey> storageInsertions = new HashSet<RecordKey>();
	
	// <Record Key -> Target transactions to be passed in local>
	private Map<RecordKey, Set<Long>> passToLocalTxns = new HashMap<RecordKey, Set<Long>>();

	private Map<Integer, Set<PushInfo>> sinkPushingInfoMap = new HashMap<Integer, Set<PushInfo>>();

	private Set<RecordKey> sinkReadingSet = new HashSet<RecordKey>();

	public SunkPlan(int sinkProcessId, boolean isHereMaster) {
		this.sinkProcessId = sinkProcessId;
		this.isHereMaster = isHereMaster;
	}

	public void addReadingInfo(RecordKey key, long srcTxNum) {
		// not need to specify dest, that is the owner tx num
		if (readingInfoMap == null)
			readingInfoMap = new HashMap<RecordKey, Long>();
		readingInfoMap.put(key, srcTxNum);
	}

	public void addPushingInfo(RecordKey key, int targetNodeId, long destTxNum) {
		if (pushingInfoMap == null)
			pushingInfoMap = new HashMap<Integer, Set<PushInfo>>();
		Set<PushInfo> pushInfos = pushingInfoMap.get(targetNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			pushingInfoMap.put(targetNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, targetNodeId, key));
	}

	public void addLocalPassingTarget(RecordKey key, long destTxNum) {
		if (passToLocalTxns.get(key) == null)
			passToLocalTxns.put(key, new HashSet<Long>());
		passToLocalTxns.get(key).add(destTxNum);
	}

	public void addSinkPushingInfo(RecordKey key, int destNodeId, long destTxNum) {
		Set<PushInfo> pushInfos = sinkPushingInfoMap.get(destNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			sinkPushingInfoMap.put(destNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, destNodeId, key));
	}

	public void addSinkReadingInfo(RecordKey key) {
		sinkReadingSet.add(key);
	}

	public Map<Integer, Set<PushInfo>> getSinkPushingInfo() {
		return sinkPushingInfoMap;
	}

	public Set<RecordKey> getSinkReadingInfo() {
		return sinkReadingSet;
	}

	public Long[] getLocalPassingTarget(RecordKey key) {
		Set<Long> set = passToLocalTxns.get(key);
		return (set == null) ? null : set.toArray(new Long[0]);
	}

	public int sinkProcessId() {
		return sinkProcessId;
	}

	public boolean isHereMaster() {
		return isHereMaster;
	}

	public void addLocalWriteBackInfo(RecordKey key) {
		localWriteBackInfo.add(key);
	}
	
	public Set<RecordKey> getReadSet() {
		if (readingInfoMap == null)
			readingInfoMap = new HashMap<RecordKey, Long>();
		return readingInfoMap.keySet();
	}

	public long getReadSrcTxNum(RecordKey key) {
		return readingInfoMap.get(key);
	}

	public Map<Integer, Set<PushInfo>> getPushingInfo() {
		return pushingInfoMap;
	}

	public List<RecordKey> getLocalWriteBackInfo() {
		return localWriteBackInfo;
	}

	public boolean hasLocalWriteBack() {
		return localWriteBackInfo.size() > 0;
	}

	public boolean hasSinkPush() {
		return sinkPushingInfoMap.size() > 0;
	}

	public void addCacheInsertion(RecordKey key) {
		cacheInsertions.add(key);
	}

	public void addCacheDeletion(RecordKey key) {
		cacheDeletions.add(key);
	}
	
	public void addStorageInsertion(RecordKey key) {
		storageInsertions.add(key);
	}

	public Set<RecordKey> getCacheInsertions() {
		return cacheInsertions;
	}

	public Set<RecordKey> getCacheDeletions() {
		return cacheDeletions;
	}
	
	public Set<RecordKey> getStorageInsertions() {
		return storageInsertions;
	}
	
	public boolean isReadOnly() {
		return localWriteBackInfo.isEmpty();
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
				RecordKey key = (RecordKey) iterator.next();
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