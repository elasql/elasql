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
package org.elasql.schedule.calvin;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.Scheduler;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class CalvinScheduler extends Task implements Scheduler {
	private static Logger logger = Logger.getLogger(CalvinScheduler.class.getName());
	
	public static final AtomicLong FIRST_TX_ARRIVAL_TIME = new AtomicLong(-1L);
	
	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();

	public CalvinScheduler(CalvinStoredProcedureFactory factory) {
		this.factory = factory;
	}

	public void schedule(StoredProcedureCall... calls) {
		try {
			for (int i = 0; i < calls.length; i++) {
				spcQueue.put(calls[i]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		StoredProcedureCall call = null;
		try {
			while (true) {
				// retrieve stored procedure call
				call = spcQueue.take();
				if (call.isNoOpStoredProcCall())
					continue;
				
				if (FIRST_TX_ARRIVAL_TIME.get() == -1L)
					FIRST_TX_ARRIVAL_TIME.set(System.currentTimeMillis());
	
				// create store procedure and prepare
				CalvinStoredProcedure<?> sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());
	
				// log request
				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);
	
				// if this node doesn't have to participate this transaction,
				// skip it
				if (!sp.isParticipating())
					continue;
	
				// serialize conservative locking
				sp.bookConservativeLocks();
	
				// create a new task for multi-thread
				CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
						call.getClientId(), call.getConnectionId(), call.getTxNum(),
						sp);
	
				// hand over to a thread to run the task
				VanillaDb.taskMgr().runTask(spt);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("detect Exception in the scheduler, current sp call: " + call);
			e.printStackTrace();
		}
	}
}
