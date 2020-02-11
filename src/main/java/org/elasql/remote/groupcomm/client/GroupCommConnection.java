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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.ElasqlSpResultSet;
import org.vanilladb.comm.client.ClientAppl;
import org.vanilladb.comm.client.ClientNodeFailListener;
import org.vanilladb.comm.client.ClientP2pMessageListener;
import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;

public class GroupCommConnection implements ClientP2pMessageListener, ClientNodeFailListener {

	// RTE id -> A blocking queue of responses from servers
	private Map<Integer, BlockingQueue<ClientResponse>> rteToRespQueue = new ConcurrentHashMap<Integer, BlockingQueue<ClientResponse>>();
	// RTE id -> The transaction number of the received response last time
	private Map<Integer, Long> rteToLastTxNum = new ConcurrentHashMap<Integer, Long>();

	private BatchSpcSender batchSender;
	private int myId;

	public GroupCommConnection(int id) {
		myId = id;

		// Initialize group communication
		ClientAppl clientAppl = new ClientAppl(id, this, this);
		clientAppl.start();
		// wait for all servers to start up
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		clientAppl.startPFD();

		// Start the batch sender
		batchSender = new BatchSpcSender(id, clientAppl);
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
	public void onRecvClientP2pMessage(P2pMessage p2pmsg) {
		ClientResponse c = (ClientResponse) p2pmsg.getMessage();
		
		// Check if this response is for this node
		if (c.getClientId() == myId) {
			rteToRespQueue.get(c.getRteId()).add(c);
		} else {
			throw new RuntimeException("Something wrong");
		}
	}

	@Override
	public void onNodeFail(int id, ChannelType channelType) {
		// do nothing
	}
}
