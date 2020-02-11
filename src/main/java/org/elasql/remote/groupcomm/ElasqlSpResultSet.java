package org.elasql.remote.groupcomm;

import java.io.Serializable;

import org.elasql.server.Elasql;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class ElasqlSpResultSet implements Serializable {
	
	private static final long serialVersionUID = 20200122001L;
	
	private SpResultSet resultSet;
	private int creatorNodeId;

	public ElasqlSpResultSet(SpResultSet resultSet) {
		this.resultSet = resultSet;
		this.creatorNodeId = Elasql.serverId();
	}
	
	public int getCreatorNodeId() {
		return creatorNodeId;
	}
	
	public SpResultSet getResultSet() {
		return resultSet;
	}
}
