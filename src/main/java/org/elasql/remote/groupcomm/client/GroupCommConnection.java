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

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.ElasqlSpResultSet;
import org.vanilladb.comm.client.VanillaCommClient;
import org.vanilladb.comm.client.VanillaCommClientListener;
import org.vanilladb.comm.view.ProcessType;

public class GroupCommConnection implements VanillaCommClientListener {

	// RTE id -> A blocking queue of responses from servers
	private Map<Integer, BlockingQueue<ClientResponse>> rteToRespQueue = new ConcurrentHashMap<Integer, BlockingQueue<ClientResponse>>();
	// RTE id -> The transaction number of the received response last time
	private Map<Integer, Long> rteToLastTxNum = new ConcurrentHashMap<Integer, Long>();
	
	private VanillaCommClient commClient;
	private BatchSpcSender batchSender;
	private int myId;
	private DirectMessageListener directMessageListener;

	public GroupCommConnection(int id, DirectMessageListener directMessageListener) {
		this.myId = id;
		this.directMessageListener = directMessageListener;

		// Initialize group communication
		commClient = new VanillaCommClient(id, this);
		
		// Create a thread for it
		new Thread(null, commClient, "VanillaComm-Clinet").start();

		// Start the batch sender
		batchSender = new BatchSpcSender(id, commClient);
		new Thread(null, batchSender, "Batch-Spc-Sender").start();
	}

	public ElasqlSpResultSet callStoredProc(int connId, int pid, Object... pars) {
		// Check if there is a queue for it
		BlockingQueue<ClientResponse> respQueue = rteToRespQueue.get(connId);
		if (respQueue == null) {
			respQueue = new LinkedBlockingQueue<ClientResponse>();
			rteToRespQueue.put(connId, respQueue);
		}

		batchSender.callStoredProc(connId, pid, pars);

		// Wait for the response
		try {
			ClientResponse cr = respQueue.take();
			Long lastTxNumObj = rteToLastTxNum.get(connId);
			long lastTxNum = -1;
			if (lastTxNumObj != null)
				lastTxNum = lastTxNumObj;

			while (lastTxNum >= cr.getTxNum())
				cr = respQueue.take();

			// Record the tx number of the response
			rteToLastTxNum.put(connId, cr.getTxNum());
			
			return cr.getResultSet();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException("Something wrong");
		}
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.SERVER) {
			ClientResponse c = (ClientResponse) message;
			
			// Check if this response is for this node
			if (c.getClientId() == myId) {
				rteToRespQueue.get(c.getRteId()).add(c);
			} else {
				throw new RuntimeException("Something wrong");
			}
		} else {
			directMessageListener.onReceivedDirectMessage(message);
		}
	}
	
	public void sendP2pMessageToClientNode(int clientId, Serializable message) {
		commClient.sendP2pMessage(ProcessType.CLIENT, clientId, message);
	}
	
	public int getServerCount() {
		return VanillaCommClient.getServerCount();
	}
	
	public int getClientCount() {
		return VanillaCommClient.getClientCount();
	}
}
