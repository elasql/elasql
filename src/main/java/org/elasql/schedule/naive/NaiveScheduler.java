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
package org.elasql.schedule.naive;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.DdStoredProcedure;
import org.elasql.schedule.Scheduler;
import org.elasql.server.task.naive.NaiveStoredProcedureTask;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class NaiveScheduler extends Task implements Scheduler {
	private static final Class<?> FACTORY_CLASS;

	private NaiveStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
	
	static {
		FACTORY_CLASS = ElasqlProperties.getLoader().getPropertyAsClass(
				NaiveScheduler.class.getName() + ".FACTORY_CLASS", null,
				NaiveStoredProcedureFactory.class);
		if (FACTORY_CLASS == null)
			throw new RuntimeException("Factory property is empty");
	}

	public NaiveScheduler() {
		try {
			factory = (NaiveStoredProcedureFactory) FACTORY_CLASS.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
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
				DdStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());

				// log request
				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);

				// create a new task for multi-thread
				NaiveStoredProcedureTask spt = new NaiveStoredProcedureTask(
						call.getClientId(), call.getRteId(), call.getTxNum(),
						sp);

				// perform conservative locking
				spt.lockConservatively();

				// hand over to a thread to run the task
				VanillaDb.taskMgr().runTask(spt);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
