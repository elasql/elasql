/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.procedure;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public abstract class StoredProcedureTask<S extends StoredProcedure<?>> extends Task {
	// Notify the listeners after a sp ends
	private static ConcurrentLinkedQueue<SpEndListener> spEndListeners = new ConcurrentLinkedQueue<SpEndListener>();
	
	public static void registerListener(SpEndListener spEndListener) {
		spEndListeners.add(spEndListener);
	}	
		
	protected S sp;
	protected int clientId;
	protected int connectionId;
	protected long txNum;

	public StoredProcedureTask(int cid, int connId, long txNum, S sp) {
		this.txNum = txNum;
		this.clientId = cid;
		this.connectionId = connId;
		this.sp = sp;
	}

	public abstract void run();

	public long getTxNum() {
		return txNum;
	}
	
	protected void onSpCommit(int txNum) {
		if (spEndListeners.isEmpty()) {
			return;
		}
		
		Iterator<SpEndListener> iterator = spEndListeners.iterator();
		while(iterator.hasNext()) {
			iterator.next().onSpCommit(txNum);
		}
	}
	
	protected void onSpRollback(int txNum) {
		if (spEndListeners.isEmpty()) {
			return;
		}
		
		Iterator<SpEndListener> iterator = spEndListeners.iterator();
		while(iterator.hasNext()) {
			iterator.next().onSpRollback(txNum);
		}
	}
}
