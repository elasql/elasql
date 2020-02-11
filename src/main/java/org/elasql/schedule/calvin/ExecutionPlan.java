package org.elasql.schedule.calvin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.RecordKey;

public class ExecutionPlan {
	
	public enum ParticipantRole { ACTIVE, PASSIVE, IGNORE };
	
	public static class PushSet {
		Set<RecordKey> keys;
		Set<Integer> nodeIds;
		
		public PushSet(Set<RecordKey> keys, Set<Integer> nodeIds) {
			this.keys = keys;
			this.nodeIds = nodeIds;
		}
		
		public Set<RecordKey> getPushKeys() {
			return keys;
		}
		
		public Set<Integer> getPushNodeIds() {
			return nodeIds;
		}
		
		@Override
		public String toString() {
			return "Keys: " + keys + ", targets: " + nodeIds;
		}
	}
	
	private ParticipantRole role = ParticipantRole.IGNORE;
	
	// Record keys for normal operations
	private Set<RecordKey> localReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> remoteReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localUpdateKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localInsertKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localDeleteKeys = new HashSet<RecordKey>();
	private Map<Integer, Set<RecordKey>> pushSets = new HashMap<Integer, Set<RecordKey>>();
	
	// For foreground migrations
	private Set<RecordKey> localReadsForMigration = new HashSet<RecordKey>();
	private Map<Integer, Set<RecordKey>> migrationPushSets = new HashMap<Integer, Set<RecordKey>>();
	private Set<RecordKey> incomingMigratingKeys = new HashSet<RecordKey>();
	
	// only for pulling migrations (Squall)
	private Set<Integer> pullingSources = new HashSet<Integer>();
	
	private boolean forceReadWriteTx = false;
	private boolean forceRemoteReadEnabled = false;

	public void addLocalReadKey(RecordKey key) {
		localReadKeys.add(key);
	}
	
	public void addRemoteReadKey(RecordKey key) {
		remoteReadKeys.add(key);
	}
	
	public void addLocalUpdateKey(RecordKey key) {
		localUpdateKeys.add(key);
	}

	public void addLocalInsertKey(RecordKey key) {
		localInsertKeys.add(key);
	}

	public void addLocalDeleteKey(RecordKey key) {
		localDeleteKeys.add(key);
	}
	
	public void addPushSet(Integer targetNodeId, RecordKey key) {
		Set<RecordKey> keys = pushSets.get(targetNodeId);
		if (keys == null) {
			keys = new HashSet<RecordKey>();
			pushSets.put(targetNodeId, keys);
		}
		keys.add(key);
	}
	
	public void addReadsForMigration(RecordKey key) {
		localReadsForMigration.add(key);
	}
	
	public void addMigrationPushSet(Integer targetNodeId, RecordKey key) {
		Set<RecordKey> keys = migrationPushSets.get(targetNodeId);
		if (keys == null) {
			keys = new HashSet<RecordKey>();
			migrationPushSets.put(targetNodeId, keys);
		}
		keys.add(key);
	}
	
	public void addImcomingMigratingKeys(RecordKey key) {
		incomingMigratingKeys.add(key);
	}
	
	public void removeFromPushSet(Integer targetNodeId, RecordKey key) {
		Set<RecordKey> keys = pushSets.get(targetNodeId);
		if (keys != null) {
			keys.remove(key);
			
			if (keys.size() == 0)
				pushSets.remove(targetNodeId);
		}
	}
	
	public void addPullingSource(Integer nodeId) {
		pullingSources.add(nodeId);
	}
	
	public void setForceReadWriteTx() {
		forceReadWriteTx = true;
	}
	
	public void setRemoteReadEnabled() {
		forceRemoteReadEnabled = true;
	}
	
	public Set<RecordKey> getLocalReadKeys() {
		return localReadKeys;
	}
	
	public Set<RecordKey> getRemoteReadKeys() {
		return remoteReadKeys;
	}
	
	public boolean isLocalUpdate(RecordKey key) {
		return localUpdateKeys.contains(key);
	}
	
	public Set<RecordKey> getLocalUpdateKeys() {
		return localUpdateKeys;
	}
	
	public boolean isLocalInsert(RecordKey key) {
		return localInsertKeys.contains(key);
	}
	
	public Set<RecordKey> getLocalInsertKeys() {
		return localInsertKeys;
	}
	
	public boolean isLocalDelete(RecordKey key) {
		return localDeleteKeys.contains(key);
	}
	
	public Set<RecordKey> getLocalDeleteKeys() {
		return localDeleteKeys;
	}
	
	public Map<Integer, Set<RecordKey>> getPushSets() {
		return pushSets;
	}
	
	public boolean hasMigrations() {
		return !localReadsForMigration.isEmpty() || !migrationPushSets.isEmpty()
				|| !incomingMigratingKeys.isEmpty();
	}
	
	public Set<RecordKey> getLocalReadsForMigration() {
		return localReadsForMigration;
	}
	
	public Map<Integer, Set<RecordKey>> getMigrationPushSets() {
		return migrationPushSets;
	}
	
	public Set<RecordKey> getIncomingMigratingKeys() {
		return incomingMigratingKeys;
	}
	
	public boolean isPullingMigration() {
		return !pullingSources.isEmpty();
	}
	
	public Set<Integer> getPullingSources() {
		return pullingSources;
	}
	
	public void setParticipantRole(ParticipantRole role) {
		this.role = role;
	}
	
	public ParticipantRole getParticipantRole() {
		return role;
	}
	
	public boolean isReadOnly() {
		if (forceReadWriteTx)
			return false;
		
		return localUpdateKeys.isEmpty() && localInsertKeys.isEmpty() &&
				localDeleteKeys.isEmpty() && incomingMigratingKeys.isEmpty();
	}
	
	public boolean hasLocalReads() {
		return !localReadKeys.isEmpty();
	}
	
	public boolean hasRemoteReads() {
		if (forceRemoteReadEnabled)
			return true;
		
		return !remoteReadKeys.isEmpty() || !incomingMigratingKeys.isEmpty();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("============== Execution Plan ==============");
		sb.append('\n');
		sb.append("Role: " + role);
		sb.append('\n');
		sb.append("Local Reads: " + localReadKeys);
		sb.append('\n');
		sb.append("Remote Reads: " + remoteReadKeys);
		sb.append('\n');
		sb.append("Local Updates: " + localUpdateKeys);
		sb.append('\n');
		sb.append("Local Inserts: " + localInsertKeys);
		sb.append('\n');
		sb.append("Local Deletes: " + localDeleteKeys);
		sb.append('\n');
		sb.append("Push Sets: " + pushSets);
		sb.append('\n');
		sb.append("Local Reads for Migration: " + localReadsForMigration);
		sb.append('\n');
		sb.append("Migration Push Sets: " + migrationPushSets);
		sb.append('\n');
		sb.append("Imcoming Migrating Keys: " + incomingMigratingKeys);
		sb.append('\n');
		sb.append("===========================================");
		sb.append('\n');
		
		return sb.toString();
	}
}
