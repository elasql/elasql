package org.elasql.procedure;

public interface SpEndListener {
	
	public void onSpCommit(int txNum);

	public void onSpRollback(int txNum);
}
