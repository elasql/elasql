package org.elasql.migration.mgcrab;

import java.io.Serializable;

public enum Phase implements Serializable {
	
	NORMAL, CATCHING_UP, CRABBING, CAUGHT_UP;
	
	private static final long serialVersionUID = 20181031001l;
}
