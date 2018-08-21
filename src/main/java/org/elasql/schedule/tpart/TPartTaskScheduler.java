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
package org.elasql.schedule.tpart;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class TPartTaskScheduler extends Task {
	private static Logger logger = Logger.getLogger(TPartTaskScheduler.class.getName());
	private BlockingQueue<Iterator<TPartStoredProcedureTask>> plansQueue;

	public TPartTaskScheduler() {
		plansQueue = new LinkedBlockingQueue<Iterator<TPartStoredProcedureTask>>();
	}

	/**
	 * Add the sunk plans of stored procedure tasks to the scheduler queue
	 * 
	 * @param task
	 */

	// Test if this class is needed
	public void addTask(Iterator<TPartStoredProcedureTask> plans) {
		try {
			plansQueue.put(plans);
		} catch (InterruptedException ex) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("fail to insert task to queue");
		}
	}

	public void schedule(TPartStoredProcedureTask spt) {
		// create worker thread to serve the request
		VanillaDb.taskMgr().runTask(spt);
	}

	public void run() {
		while (true) {
			try {
				Iterator<TPartStoredProcedureTask> plans = plansQueue.take();
				while (plans.hasNext()) {
					TPartStoredProcedureTask p = plans.next();
					schedule(p);
				}
			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}
}
