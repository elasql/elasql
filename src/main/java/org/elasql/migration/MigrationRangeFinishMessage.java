package org.elasql.migration;

import java.io.Serializable;

public class MigrationRangeFinishMessage implements Serializable {
	
	private static final long serialVersionUID = 20181104001L;
	
	private int finishRangeCount;
	
	public MigrationRangeFinishMessage(int finishRangeCount) {
		this.finishRangeCount = finishRangeCount;
	}
	
	public int getFinishRangeCount() {
		return finishRangeCount;
	}
}
