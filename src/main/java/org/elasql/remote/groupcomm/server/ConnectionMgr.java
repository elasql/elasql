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
package org.elasql.remote.groupcomm.server;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class ConnectionMgr implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());
	
	private static class TotalOrderMessage {
		int serialNumber;
		Serializable message;
	}

	private VanillaCommServer commServer;
	private boolean sequencerMode;
	private BlockingQueue<Serializable> tomSendQueue = new LinkedBlockingQueue<Serializable>();
	private BlockingQueue<TotalOrderMessage> tomReceiveQueue = new LinkedBlockingQueue<TotalOrderMessage>();
	private boolean areAllServersReady = false;

	public ConnectionMgr(int id, boolean seqMode) {
		sequencerMode = seqMode;
		commServer = new VanillaCommServer(id, this);
		new Thread(null, commServer, "VanillaComm-Server").start();

		// Only the sequencer needs to wait for all servers ready
		if (sequencerMode) {
			waitForServersReady();
			createTomSender();
		} else {
			createTomReceiver();
		}
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		commServer.sendP2pMessage(ProcessType.CLIENT, clientId,
				new ClientResponse(clientId, rteId, txNum, rs));
	}
	
	public void sendStoredProcedureCall(boolean fromAppiaThread, int pid, Object[] pars) {
		Object[] spCalls = { new StoredProcedureCall(-1, -1, pid, pars)};
		commServer.sendTotalOrderMessage(spCalls);
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		commServer.sendP2pMessage(ProcessType.SERVER, nodeId, reading);
	}

	@Override
	public void onServerReady() {
		synchronized (this) {
			areAllServersReady = true;
			this.notifyAll();
		}
	}

	@Override
	public void onServerFailed(int failedServerId) {
		// Do nothing
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.CLIENT) {
			// Normally, the client will only sends its request to the sequencer.
			// However, any other server can also send a total order request.
			// So, we do not need to check if this machine is the sequencer.
			tomSendQueue.add(message);
		} else if (message.getClass().equals(TupleSet.class)) {
			TupleSet ts = (TupleSet) message;
			
			if (ts.sinkId() == MigrationSystemController.MSG_RANGE_FINISH) {
				Elasql.migraSysControl().onReceiveMigrationRangeFinishMsg(
						(MigrationRangeFinishMessage) ts.getMetadata());
				return;
			}
			
			for (Tuple t : ts.getTupleSet())
				Elasql.remoteRecReceiver().cacheRemoteRecord(t);
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public void onReceiveTotalOrderMessage(int serialNumber, Serializable message) {
		if (sequencerMode)
			return;

		try {
			TotalOrderMessage tom = new TotalOrderMessage();
			tom.serialNumber = serialNumber;
			tom.message = message;
			tomReceiveQueue.put(tom);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void createTomReceiver() {
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				long transactionNumber = 1;
				
				while (true) {
					try {
						TotalOrderMessage tom = tomReceiveQueue.take();
						StoredProcedureCall[] batch = (StoredProcedureCall[]) tom.message;
						for (int i = 0; i < batch.length; ++i) {
							StoredProcedureCall spc = (StoredProcedureCall) batch[i];
							spc.setTxNum(transactionNumber);
							transactionNumber++;
							Elasql.scheduler().schedule(spc);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		});
	}
	
	private void createTomSender() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Serializable message = tomSendQueue.take();
						commServer.sendTotalOrderMessage(message);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}).start();;
	}
	
	private void waitForServersReady() {
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		synchronized (this) {
			try {
				while (!areAllServersReady)
					this.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
