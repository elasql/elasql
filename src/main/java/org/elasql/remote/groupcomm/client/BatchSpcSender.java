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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.server.ConnectionMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.comm.client.VanillaCommClient;
import org.vanilladb.comm.view.ProcessType;

class BatchSpcSender implements Runnable {
	private static Logger logger = Logger.getLogger(BatchSpcSender.class.getName());

	private final static int COMM_BATCH_SIZE;
	private final static long MAX_WAITING_TIME; // in ms

	static {
		COMM_BATCH_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(BatchSpcSender.class.getName() + ".COMM_BATCH_SIZE", 1);
		MAX_WAITING_TIME = ElasqlProperties.getLoader()
				.getPropertyAsInteger(BatchSpcSender.class.getName() + ".MAX_WAITING_TIME", 1000);
	}

	private AtomicInteger numOfQueuedSpcs = new AtomicInteger(0);
	private Queue<StoredProcedureCall> spcQueue = new ConcurrentLinkedQueue<StoredProcedureCall>();
	private VanillaCommClient commClient;
	private long lastSendingTime;
	private int nodeId;

	public BatchSpcSender(int id, VanillaCommClient client) {
		commClient = client;
		lastSendingTime = System.currentTimeMillis();
		nodeId = id;
	}

	@Override
	public void run() {
		// periodically send batch of requests
		if (logger.isLoggable(Level.INFO))
			logger.info("start batching-request worker thread (batch size = " + COMM_BATCH_SIZE + ")"); 

		while (true)
			sendBatchRequestToDb();
	}

	public void callStoredProc(int connId, int pid, Object... pars) {
		StoredProcedureCall spc = new StoredProcedureCall(nodeId, connId, pid, pars);
		spcQueue.add(spc);
		int size = numOfQueuedSpcs.incrementAndGet();
		
		if (size >= COMM_BATCH_SIZE) {
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
			while (size < COMM_BATCH_SIZE && (currentTime - lastSendingTime < MAX_WAITING_TIME || size < 1)) {
				
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
		List<StoredProcedureCall> batchSpc = new ArrayList<StoredProcedureCall>(size * 2);
		while (spcQueue.peek() != null) {
			StoredProcedureCall spc = spcQueue.poll();
			batchSpc.add(spc);
		}
		numOfQueuedSpcs.addAndGet(-batchSpc.size());
		commClient.sendP2pMessage(ProcessType.SERVER, ConnectionMgr.SEQUENCER_ID, batchSpc.toArray(new StoredProcedureCall[0]));
	}
}
