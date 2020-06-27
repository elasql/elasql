package org.elasql.schedule.calvin;

import java.util.HashSet;
import java.util.Set;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class SequencerAnalyzer implements ReadWriteSetAnalyzer {
	
	private ExecutionPlan execPlan;
	
	// Sequencer only collects read keys and write keys (not including fully replicated keys)
	private Set<RecordKey> readKeys = new HashSet<RecordKey>();
	private Set<RecordKey> writeKeys = new HashSet<RecordKey>();
	
	public SequencerAnalyzer() {
		execPlan = new ExecutionPlan();
	}
	
	@Override
	public ExecutionPlan generatePlan() {
		return execPlan;
	}

	@Override
	public void addReadKey(RecordKey readKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(readKey)) {
			readKeys.add(readKey);
		}
	}
	
	@Override
	public void addUpdateKey(RecordKey updateKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(updateKey)) {
			writeKeys.add(updateKey);
		}
	}
	
	@Override
	public void addInsertKey(RecordKey insertKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(insertKey)) {
			writeKeys.add(insertKey);
		}
	}
	
	@Override
	public void addDeleteKey(RecordKey deleteKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(deleteKey)) {
			writeKeys.add(deleteKey);
		}
	}
	
	public Set<RecordKey> getReadKeys() {
		return readKeys;
	}
	
	public Set<RecordKey> getWriteKeys() {
		return writeKeys;
	}
}
