package org.elasql.schedule.calvin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
	}
	
	private ParticipantRole role = ParticipantRole.IGNORE;
	
	// Record keys
	private Set<RecordKey> localReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> remoteReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localUpdateKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localInsertKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localDeleteKeys = new HashSet<RecordKey>();
	private Set<RecordKey> insertsForMigration = new HashSet<RecordKey>();
	private List<PushSet> pushSets = new ArrayList<PushSet>();

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
	
	public void addInsertForMigration(RecordKey key) {
		insertsForMigration.add(key);
	}
	
	public void addPushSet(Set<RecordKey> keys, Set<Integer> nodeIds) {
		pushSets.add(new PushSet(new HashSet<RecordKey>(keys), nodeIds));
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
	
	public Set<RecordKey> getInsertsForMigration() {
		return insertsForMigration;
	}
	
	public List<PushSet> getPushSets() {
		return pushSets;
	}
	
	public void setParticipantRole(ParticipantRole role) {
		this.role = role;
	}
	
	public ParticipantRole getParticipantRole() {
		return role;
	}
	
	public boolean isReadOnly() {
		return localUpdateKeys.isEmpty() && localInsertKeys.isEmpty() &&
				localDeleteKeys.isEmpty();
	}
	
	public boolean hasLocalReads() {
		return !localReadKeys.isEmpty();
	}
	
	public boolean hasRemoteReads() {
		return !remoteReadKeys.isEmpty();
	}
}
