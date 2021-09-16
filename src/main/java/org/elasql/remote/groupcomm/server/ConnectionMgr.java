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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationSystemController;
import org.elasql.perf.MetricReport;
import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.server.Elasql.ServiceType;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.TransactionProfiler;

public class ConnectionMgr implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());
	
	public static final int SEQUENCER_ID = VanillaCommServer.getServerCount() - 1;

	private VanillaCommServer commServer;
	private boolean sequencerMode;
	private BlockingQueue<List<Serializable>> tomSendQueue = new LinkedBlockingQueue<List<Serializable>>();
	private boolean areAllServersReady = false;
	
	private long firstSpcArrivedTime = -1;

	// See Note #1 in onReceiveP2pMessage
	private long nextTransactionId = 1;

	public ConnectionMgr(int id) {
		sequencerMode = Elasql.serverId() == SEQUENCER_ID;
		commServer = new VanillaCommServer(id, this);
		new Thread(null, commServer, "VanillaComm-Server").start();

		// Only the sequencer needs to wait for all servers ready
		if (sequencerMode) {
			waitForServersReady();
			createTomSender();
		}
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		commServer.sendP2pMessage(ProcessType.CLIENT, clientId,
				new ClientResponse(clientId, rteId, txNum, rs));
	}
	
	public void sendStoredProcedureCall(boolean fromAppiaThread, int pid, Object[] pars) {
		commServer.sendTotalOrderMessage(new StoredProcedureCall(-1, -1, pid, pars));
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		// For controller
		TransactionProfiler.getLocalProfiler().incrementNetworkOutSize(reading);
		commServer.sendP2pMessage(ProcessType.SERVER, nodeId, reading);
	}
	
	public void sendMetricReport(MetricReport report) {
		commServer.sendP2pMessage(ProcessType.SERVER, SEQUENCER_ID, report);
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
			
			// Transfer the given batch to a list of messages
			StoredProcedureCall[] spcs = (StoredProcedureCall[]) message;
			List<Serializable> tomRequest = new ArrayList<Serializable>(spcs.length);
			for (StoredProcedureCall spc : spcs) {
				// Record when the first spc arrives
				if (firstSpcArrivedTime == -1)
					firstSpcArrivedTime = System.nanoTime();
				
				// Set arrived time
				long arrivedTime = (System.nanoTime() - firstSpcArrivedTime) / 1000;
				spc.stampArrivedTime(arrivedTime);
				
				// Set transaction number
				// Note #1: the transaction number originally should be dispatched
				// through Zab. However, in order to estimate the latency and 
				// the cost of each transaction. We make the sequencer decide
				// the transaction number before the total ordering, so that
				// we can estimate the cost before sending the requests to DB
				// servers.
				// This method works because there is only one leader in Zab,
				// and the leader won't change in the experiment.
				spc.setTxNum(nextTransactionId);
				nextTransactionId++;

				// Processes the transaction request
				// and append some metadata if necessary
				if (Elasql.performanceMgr() != null)
					Elasql.performanceMgr().preprocessSpCall(spc);
				else
					// Add to the total order request list
					tomRequest.add(spc);
			}
			
			// If there is a performance manager,
			// it will take the responsibility of sending the
			// requests to total-ordering module.
			if (Elasql.performanceMgr() == null)
				sendTotalOrderRequest(tomRequest);
			
		} else if (message.getClass().equals(TupleSet.class)) {
			TupleSet ts = (TupleSet) message;
			
			if (ts.sinkId() == MigrationSystemController.MSG_RANGE_FINISH) {
				Elasql.migraSysControl().onReceiveMigrationRangeFinishMsg(
						(MigrationRangeFinishMessage) ts.getMetadata());
				return;
			}
			
			for (Tuple t : ts.getTupleSet())
				Elasql.remoteRecReceiver().cacheRemoteRecord(t);
		} else if (message instanceof MetricReport) {
			MetricReport report = (MetricReport) message;
			Elasql.performanceMgr().receiveMetricReport(report);
		} else
			throw new IllegalArgumentException();
	}
	
	public void sendTotalOrderRequest(List<Serializable> requests) {
		// Send a total order request
		try {
			tomSendQueue.put(requests);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		StoredProcedureCall spc = (StoredProcedureCall) message;
		
		// See Note #1 in onReceiveP2pMessage
//		spc.setTxNum(serialNumber);
		
		// The sequencer running with Calvin must receive stored procedure call for planning migrations
		if (sequencerMode && Elasql.SERVICE_TYPE != ServiceType.CALVIN)
			return;
		
		// Pass to the scheduler
		Elasql.scheduler().schedule(spc);
	}
	
	private void createTomSender() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						List<Serializable> messages = tomSendQueue.take();
						commServer.sendTotalOrderMessages(messages);
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
