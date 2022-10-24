package org.elasql.perf;

import java.io.Serializable;

public interface TransactionReport extends Serializable {
	
	long getLatency();
	
}
