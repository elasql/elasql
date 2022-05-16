package org.elasql.storage.tx;

import java.util.Set;

import org.elasql.sql.PrimaryKey;

/**
 * OccTransaction acts as an extension of the existing Transaction class in
 * VanillaCore. Read/write set should be kept in the transaction; however, the
 * original Transaction class must keep the references from ElaSql and this
 * cross reference behavior violates VanillaCore's design rules.
 * 
 * @author Pin-Yu Wang
 *
 */
public class OccTransaction {
	private long txNum;
	private long readPhaseStartTime = 0;
	private long writePhaseEndTime = 0;
	private Set<PrimaryKey> readSet = null;
	private Set<PrimaryKey> writeSet = null;

	public OccTransaction(long txNum) {
		this.txNum = txNum;
	}

	public void setReadPhaseStartTime() {
		readPhaseStartTime = System.nanoTime();
	}

	public void setWritePhaseEndTime() {
		writePhaseEndTime = System.nanoTime();
	}

	public void setReadWriteSet(Set<PrimaryKey> readSet, Set<PrimaryKey> writeSet) {
		this.readSet = readSet;
		this.writeSet = writeSet;
	}

	public long getReadPhaseStartTime() {
		return readPhaseStartTime;
	}

	public long getWritePhaseEndTime() {
		return writePhaseEndTime;
	}

	public Set<PrimaryKey> getReadSet() {
		return readSet;
	}

	public Set<PrimaryKey> getWriteSet() {
		return writeSet;
	}

	public long getTxNum() {
		return txNum;
	}
}
