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

import java.io.IOException;
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
import org.elasql.remote.groupcomm.CommitNotification;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.server.Elasql.ServiceType;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.TransactionProfiler;

public class ConnectionMgr implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());
	
	public static final int SEQUENCER_ID = VanillaCommServer.getServerCount() - 1;

	private final static long NETWORK_LATENCY; 
	static { 
		NETWORK_LATENCY = ElasqlProperties.getLoader().getPropertyAsLong(ConnectionMgr.class.getName() + ".NETWORK_LATENCY", 0); 
	} 
	
	private VanillaCommServer commServer;
	private boolean isSequencer;
	private BlockingQueue<List<Serializable>> tomSendQueue = new LinkedBlockingQueue<List<Serializable>>();
	private boolean areAllServersReady = false;
	
	private long firstSpcArrivedTime = -1;

	// See Note #1 in onReceiveP2pMessage
	private long nextTransactionId = 1;
	private List<Serializable> tomRequest = new ArrayList<Serializable>();

	public ConnectionMgr(int id) {
		isSequencer = Elasql.serverId() == SEQUENCER_ID;
		commServer = new VanillaCommServer(id, this);
	}
	
	public void start() {
		new Thread(null, commServer, "VanillaComm-Server").start();

		// Only the sequencer needs to wait for all servers ready
		if (isSequencer) {
			waitForServersReady();
			createTomSender();
		}
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		commServer.sendP2pMessage(ProcessType.CLIENT, clientId,
				new ClientResponse(clientId, rteId, txNum, rs));
	}
	
	public void sendCommitNotification(long txNum, long txLatency) {
		commServer.sendP2pMessage(ProcessType.SERVER, SEQUENCER_ID, 
				new CommitNotification(txNum, Elasql.serverId(), txLatency));
	}
	
	public void sendStoredProcedureCall(boolean fromAppiaThread, int pid, Object[] pars) {
		// In order to ensure that all the transaction numbers are dispatched
		// from the sequencer, all stored procedure calls must be sent to the sequencer
		// for further processing.
		StoredProcedureCall spc = new StoredProcedureCall(-1, -1, pid, pars);
		if (isSequencer) {
			onReceiveP2pMessage(ProcessType.SERVER, Elasql.serverId(), spc);
		} else {
			commServer.sendP2pMessage(ProcessType.SERVER, SEQUENCER_ID, spc);
		}
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		// For controller
		TransactionProfiler.getLocalProfiler().incrementNetworkOutSize(reading);
		
		if (NETWORK_LATENCY > 0) {
			try { 
				Thread.sleep(NETWORK_LATENCY); 
			} catch (InterruptedException e) { 
				e.printStackTrace(); 
			} 
		}
		
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
		if (isSequencer) {
			if (message.getClass().equals(StoredProcedureCall[].class)) {
				StoredProcedureCall[] spcs = (StoredProcedureCall[]) message;
				for (StoredProcedureCall spc : spcs) {
					onReceivedSpCall(spc);
				}
				flushTotalOrderMsgs();
			} else if (message.getClass().equals(StoredProcedureCall.class)) {
				StoredProcedureCall spc = (StoredProcedureCall) message;
				onReceivedSpCall(spc);
				flushTotalOrderMsgs();
			} else if (message.getClass().equals(CommitNotification.class)) {
				CommitNotification cn = (CommitNotification) message;
				Elasql.performanceMgr().onTransactionCommit(cn.getTxNum(),
						cn.getMasterId(), cn.getTxLatency());
			} else if (message instanceof MetricReport) {
				MetricReport report = (MetricReport) message;
				Elasql.performanceMgr().receiveMetricReport(report);
			} else
				throw new IllegalArgumentException("the sequencer doesn't know how to handle "
						+ message);
		} else {
			if (message.getClass().equals(TupleSet.class)) {
				TupleSet ts = (TupleSet) message;
				
				if (ts.sinkId() == MigrationSystemController.MSG_RANGE_FINISH) {
					Elasql.migraSysControl().onReceiveMigrationRangeFinishMsg(
							(MigrationRangeFinishMessage) ts.getMetadata());
					return;
				}
				
				for (Tuple t : ts.getTupleSet())
					Elasql.remoteRecReceiver().cacheRemoteRecord(t);
			} else
				throw new IllegalArgumentException("server #" + Elasql.serverId() + 
						" doesn't know how to handle " + message);
		}
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
		
		profileBroadcast(spc, message);
		
		// See Note #1 in onReceivedSpCall
//		spc.setTxNum(serialNumber);
		
		// The sequencer running with Calvin must receive stored procedure call for planning migrations
		if (isSequencer && Elasql.SERVICE_TYPE != ServiceType.CALVIN)
			return;
		
		// Pass to the scheduler
		Elasql.scheduler().schedule(spc);
	}
	
	public boolean areAllServersReady() {
		return areAllServersReady;
	}
	
	private void onReceivedSpCall(StoredProcedureCall spc) {
		// Record when the first spc arrives
		if (firstSpcArrivedTime == -1)
			firstSpcArrivedTime = System.nanoTime();
		
		// Set arrived time
		long arrivedTime = (System.nanoTime() - firstSpcArrivedTime) / 1000;
		spc.stampArrivedTime(arrivedTime);
		spc.stampOu0StartTime(System.nanoTime());
		
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
			// Add to the total order request queue
			tomRequest.add(spc);
	}
	
	private void flushTotalOrderMsgs() {
		if (!tomRequest.isEmpty()) {
			sendTotalOrderRequest(tomRequest);
			tomRequest = new ArrayList<Serializable>();
		}	
	}
	
	private void profileBroadcast(StoredProcedureCall spc, Serializable message) {
		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		profiler.reset();
		profiler.startExecution();
		
		long broadcastTime = (spc.getOu0StopTime() - spc.getOu0StartTime()) / 1000;
		int networkSize = 0;
		try {
			networkSize = TransactionProfiler.getMessageSize(message);
		} catch (IOException e) {
			e.printStackTrace();
		}
		profiler.addComponentProfile("OU0 - Broadcast", broadcastTime, 0, 0, networkSize, 0, 0);
		profiler.startComponentProfiler("OU0 - Dispatch to router");
		
		spc.setProfiler(TransactionProfiler.takeOut());
	}
	
	private void createTomSender() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						List<Serializable> messages = tomSendQueue.take();
						for (int i = messages.size() - 1; i >= 0; i--) {
							StoredProcedureCall spc = (StoredProcedureCall) messages.get(i);
							spc.stampOu0StopTime(System.nanoTime());
						}
						commServer.sendTotalOrderMessages(messages);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}).start();
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
