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
package org.elasql.schedule.calvin;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.cache.calvin.CalvinRemotePostOffice;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.Scheduler;
import org.elasql.server.Elasql;
import org.elasql.server.task.calvin.CalvinStoredProcedureTask;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class CalvinScheduler extends Task implements Scheduler {
	private static final Class<?> FACTORY_CLASS;

	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
	private CalvinRemotePostOffice postOffice;

	static {
		FACTORY_CLASS = ElasqlProperties.getLoader().getPropertyAsClass(
				CalvinScheduler.class.getName() + ".FACTORY_CLASS", null,
				CalvinStoredProcedureFactory.class);
		if (FACTORY_CLASS == null)
			throw new RuntimeException("Factory property is empty");
	}

	public CalvinScheduler() {
		try {
			factory = (CalvinStoredProcedureFactory) FACTORY_CLASS.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		postOffice = (CalvinRemotePostOffice) Elasql.remoteRecReceiver();
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
		while (true) {
			try {
				// retrieve stored procedure call
				StoredProcedureCall call = spcQueue.take();
				if (call.isNoOpStoredProcCall())
					continue;

				// create store procedure and prepare
				CalvinStoredProcedure<?> sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());

				// log request
				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);

				// if this node doesn't have to participate this transaction,
				// skip it
				if (!sp.isParticipated()) {
					postOffice.skipTransaction(sp.txNum);
					continue;
				}

				// create a new task for multi-thread
				CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
						call.getClientId(), call.getRteId(), call.getTxNum(),
						sp);

				// perform conservative locking
				spt.bookConservativeLocks();

				// hand over to a thread to run the task
				VanillaDb.taskMgr().runTask(spt);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
