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
package org.elasql.remote.groupcomm.client;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.comm.client.VanillaCommClient;
import org.vanilladb.comm.view.ProcessType;

class BatchSpcSender implements Runnable {
	private static Logger logger = Logger.getLogger(BatchSpcSender.class.getName());

	private final static int BATCH_SIZE;
	private final static long MAX_WAITING_TIME; // in ms

	static {
		BATCH_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(BatchSpcSender.class.getName() + ".BATCH_SIZE", 1);
		MAX_WAITING_TIME = ElasqlProperties.getLoader()
				.getPropertyAsInteger(BatchSpcSender.class.getName() + ".MAX_WAITING_TIME", 1000);
	}

	private AtomicInteger numOfQueuedSpcs = new AtomicInteger(0);
	private Queue<StoredProcedureCall> spcQueue = new ConcurrentLinkedQueue<StoredProcedureCall>();
	private VanillaCommClient commClient;
	private long lastSendingTime;
	private int nodeId, sequencerId;

	public BatchSpcSender(int id, VanillaCommClient client) {
		commClient = client;
		lastSendingTime = System.currentTimeMillis();
		nodeId = id;
		sequencerId = client.getServerCount() - 1;
	}

	@Override
	public void run() {
		// periodically send batch of requests
		if (logger.isLoggable(Level.INFO))
			logger.info("start batching-request worker thread");

		while (true)
			sendBatchRequestToDb();
	}

	public void callStoredProc(int connId, int pid, Object... pars) {
		StoredProcedureCall spc = new StoredProcedureCall(nodeId, connId, pid, pars);
		spcQueue.add(spc);
		int size = numOfQueuedSpcs.incrementAndGet();
		
		if (size >= BATCH_SIZE) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	private void sendBatchRequestToDb() {
		// Waiting for the queue reaching the threshold
		int size = numOfQueuedSpcs.get();
		long currentTime = System.currentTimeMillis();
		try {
			while (size < BATCH_SIZE && (currentTime - lastSendingTime < MAX_WAITING_TIME || size < 1)) {
				
				synchronized (this) {
					wait(100);
				}
				
				size = numOfQueuedSpcs.get();
				currentTime = System.currentTimeMillis();
			}
			
			lastSendingTime = currentTime;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Send a batch of requests
		StoredProcedureCall[] batchSpc = new StoredProcedureCall[size];
		StoredProcedureCall spc;
		for (int i = 0; i < batchSpc.length; i++) {
			spc = spcQueue.poll();
			
			if (spc == null)
				throw new RuntimeException("Something wrong");
			
			batchSpc[i] = spc;
		}
		numOfQueuedSpcs.addAndGet(-size);
		commClient.sendP2pMessage(ProcessType.SERVER, sequencerId, batchSpc);
	}
}
