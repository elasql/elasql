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
package org.elasql.procedure.calvin;

import org.elasql.procedure.StoredProcedureTask;
import org.elasql.server.Elasql;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class CalvinStoredProcedureTask
		extends StoredProcedureTask<CalvinStoredProcedure<?>> {
	
	static {
		// For Debugging
//		TimerStatistics.startReporting();
	}

	public CalvinStoredProcedureTask(
			int cid, int connId, long txNum,
			CalvinStoredProcedure<?> sp) {
		super(cid, connId, txNum, sp);
	}

	public void run() {
		Thread.currentThread().setName("Tx." + txNum + " (running)");
		
//		Timer timer = Timer.getLocalTimer();
		SpResultSet rs = null;
		
//		timer.reset();
//		timer.startExecution();

//		try {
			rs = sp.execute();
//		} finally {
//			timer.stopExecution();
//		}
		
//		if (txNum % 100 == 0) {
//			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
//			System.out.println(String.format("Tx.%d commits at %d ms.", txNum, time));
//		}

		if (sp.willResponseToClients()) {
			Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);
		}
		
		// For Debugging
//		if (timer.getExecutionTime() > 1000_000)
//			System.out.println("Tx:" + txNum + "'s Timer:\n" + timer.toString());
//		timer.addToGlobalStatistics();
		
		Thread.currentThread().setName("Tx." + txNum + " (committed)");
	}
}
