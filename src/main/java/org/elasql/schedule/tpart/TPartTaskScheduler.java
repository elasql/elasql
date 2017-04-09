package org.elasql.schedule.tpart;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class TPartTaskScheduler extends Task {
	private static Logger logger = Logger.getLogger(TPartTaskScheduler.class
			.getName());
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
		// spt.lockConservatively();

		// create worker thread to serve the request
		VanillaDb.taskMgr().runTask(spt);
	}

	public void run() {
		while (true) {
			try {
				Iterator<TPartStoredProcedureTask> plans = plansQueue.take();
				// System.out.println("sche");
				while (plans.hasNext()) {
					TPartStoredProcedureTask p = plans.next();
					// System.out.println("schedule: " + p.getTxNum());
					schedule(p);
				}
			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}
}
