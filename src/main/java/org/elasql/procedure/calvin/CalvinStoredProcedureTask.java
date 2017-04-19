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

public class CalvinStoredProcedureTask extends StoredProcedureTask {

	private CalvinStoredProcedure<?> csp;
	//private static long startTime = System.nanoTime();

	public CalvinStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);

		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {
		// Timers.createTimer(txNum);
		SpResultSet rs = null;
		// Timers.getTimer().startExecution();

		// try {
		rs = sp.execute();
		// } finally {
		// Timers.getTimer().stopExecution();
		// }

		if (csp.willResponseToClients()) {
			// System.out.println("Commit: " + (System.nanoTime() - startTime));
			Elasql.connectionMgr().sendClientResponse(cid, rteId, txNum,
					rs);
		}
		
		// Timers.reportTime();
	}

	public void bookConservativeLocks() {
		csp.bookConservativeLocks();
	}
}