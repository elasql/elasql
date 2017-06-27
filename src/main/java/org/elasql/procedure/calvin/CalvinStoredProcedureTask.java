/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.procedure.calvin;

import org.elasql.procedure.DdStoredProcedure;
import org.elasql.procedure.StoredProcedureTask;
import org.elasql.server.Elasql;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.Timer;

public class CalvinStoredProcedureTask extends StoredProcedureTask {

	static {
		// For Debugging
		// TimerStatistics.startReporting();
	}
	public static long txStartTime = 0;
	private CalvinStoredProcedure<?> csp;

	public CalvinStoredProcedureTask(int cid, int connId, long txNum, DdStoredProcedure sp) {
		super(cid, connId, txNum, sp);

		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {

		if (txStartTime == 0)
			txStartTime = System.currentTimeMillis();

		Timer timer = Timer.getLocalTimer();
		SpResultSet rs = null;

		timer.reset();
		timer.startExecution();

		try {
			rs = sp.execute();
		} finally {
			timer.stopExecution();
		}

		if (csp.willResponseToClients()) {
			Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);
		}

		// For Debugging
		// System.out.println("Tx:" + txNum + "'s Timer:\n" + timer.toString());
		// timer.addToGlobalStatistics();
	}

	public void bookConservativeLocks() {
		csp.bookConservativeLocks();
	}
}
