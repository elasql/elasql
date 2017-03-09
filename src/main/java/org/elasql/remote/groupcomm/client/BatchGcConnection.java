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
package org.elasql.remote.groupcomm.client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.comm.client.ClientAppl;
import org.vanilladb.comm.client.ClientNodeFailListener;
import org.vanilladb.comm.client.ClientP2pMessageListener;
import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class BatchGcConnection implements ClientP2pMessageListener,
		ClientNodeFailListener, Runnable {
	private static Logger logger = Logger.getLogger(BatchGcConnection.class
			.getName());

	private int myId;
	private final static int BATCH_SIZE;
	private ClientAppl clientAppl;
	private long lastTime = System.nanoTime();
	private long testTime, testTime2;
	private int testRte = -1, testRte2 = -1;

	private Queue<StoredProcedureCall> spcQueue = new LinkedList<StoredProcedureCall>();
	private Map<Long, ClientResponse> txnRespMap = new HashMap<Long, ClientResponse>();
	private Map<Integer, Long> rteIdtoTxNumMap = new HashMap<Integer, Long>();

	static {
		BATCH_SIZE = ElasqlProperties.getLoader().getPropertyAsInteger(
				BatchGcConnection.class.getName() + ".BATCH_SIZE", 1);
	}

	public BatchGcConnection(int id) {
		ClientAppl clientAppl = new ClientAppl(id, this, this);
		this.clientAppl = clientAppl;
		myId = id;

		clientAppl.start();
		// wait for all servers to start up
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		clientAppl.startPFD();
	}

	@Override
	public void run() {
		// periodically send batch of requests
		if (logger.isLoggable(Level.INFO))
			logger.info("start batching-request worker");

		while (true) {
			sendBatchRequestToDb();
			// try {
			// Thread.sleep(RESEND_INTERVAL);
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}

	}

	public synchronized SpResultSet callStoredProc(int rteId, int pid,
			Object... pars) {
		// if (testRte == -1) {
		// testTime = System.nanoTime();
		// testRte = rteId;
		// }
		// System.out.println("call proc rte:" + rteId);
		// block the calling thread until receiving corresponding request
		if (!rteIdtoTxNumMap.containsKey(rteId)) {
			rteIdtoTxNumMap.put(rteId, -1L);
		}
		StoredProcedureCall spc = new StoredProcedureCall(myId, rteId, pid,
				pars);
		spcQueue.add(spc);
		notifyAll();
		ClientResponse cr;
		try {
			while (true) {
				Long txNum = rteIdtoTxNumMap.get(rteId);
				if (txnRespMap.containsKey(txNum)) {
					cr = txnRespMap.remove(txNum);
					break;
				}
				wait();
			}
			// System.out.println("rte " + rteId + " recv.:");
			// if (rteId == testRte2) {
			// System.out.println("recv time:"
			// + (System.nanoTime() - testTime2));
			// testRte2 = -1;
			// }
			return (SpResultSet) cr.getResultSet();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
	}

	@Override
	public synchronized void onRecvClientP2pMessage(P2pMessage p2pmsg) {
		ClientResponse c = (ClientResponse) p2pmsg.getMessage();
		long txNum = c.getTxNum();
		if (c.getClientId() == myId) {
			long oldTxNum = rteIdtoTxNumMap.get(c.getRteId());
			if (txNum > oldTxNum) {
				rteIdtoTxNumMap.put(c.getRteId(), txNum);
				txnRespMap.put(txNum, c);
				notifyAll();
			}
		}
		// if (testRte2 == -1) {
		// testRte2 = c.getRteId();
		// testTime2 = System.nanoTime();
		// }
	}

	private synchronized void sendBatchRequestToDb() {
		long time = System.nanoTime();
		try {
			while (spcQueue.size() < BATCH_SIZE
					&& (time - lastTime < 1000000000L || spcQueue.size() < 1)) {
				wait(1000);
				time = System.nanoTime();
			}
			lastTime = time;
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		StoredProcedureCall[] batchSpc = new StoredProcedureCall[spcQueue
				.size()];
		StoredProcedureCall spc;
		for (int i = 0; i < batchSpc.length; i++) {
			if ((spc = spcQueue.poll()) != null)
				batchSpc[i] = spc;
			// if (spc.getRteId() == testRte) {
			// System.out.println("send Time:"
			// + (System.nanoTime() - testTime));
			// testRte = -1;
			// }
			// else
			// batchSpc[i] = StoredProcedureCall.getNoOpStoredProcCall(myId);
		}
		clientAppl.sendRequest(batchSpc);
	}

	@Override
	public void onNodeFail(int id, ChannelType channelType) {
		// do nothing
	}
}
