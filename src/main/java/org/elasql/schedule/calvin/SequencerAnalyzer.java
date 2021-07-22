package org.elasql.schedule.calvin;

import java.util.HashSet;
import java.util.Set;

import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;

public class SequencerAnalyzer implements ReadWriteSetAnalyzer {
	
	private ExecutionPlan execPlan;
	
	// Sequencer only collects read keys and write keys (not including fully replicated keys)
	private Set<PrimaryKey> readKeys = new HashSet<PrimaryKey>();
	private Set<PrimaryKey> writeKeys = new HashSet<PrimaryKey>();
	
	public SequencerAnalyzer() {
		execPlan = new ExecutionPlan();
	}
	
	@Override
	public ExecutionPlan generatePlan() {
		return execPlan;
	}

	@Override
	public void addReadKey(PrimaryKey readKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(readKey)) {
			readKeys.add(readKey);
		}
	}
	
	@Override
	public void addUpdateKey(PrimaryKey updateKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(updateKey)) {
			writeKeys.add(updateKey);
		}
	}
	
	@Override
	public void addInsertKey(PrimaryKey insertKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(insertKey)) {
			writeKeys.add(insertKey);
		}
	}
	
	@Override
	public void addDeleteKey(PrimaryKey deleteKey) {
		if (!Elasql.partitionMetaMgr().isFullyReplicated(deleteKey)) {
			writeKeys.add(deleteKey);
		}
	}
	
	public Set<PrimaryKey> getReadKeys() {
		return readKeys;
	}
	
	public Set<PrimaryKey> getWriteKeys() {
		return writeKeys;
	}
}
