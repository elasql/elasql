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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationSystemController;
import org.elasql.perf.MetricReport;
import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.SyncRequest;
import org.elasql.remote.groupcomm.TimeSync;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.server.Elasql.ServiceType;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class ConnectionMgr implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());
	
	public static final int SEQUENCER_ID = VanillaCommServer.getServerCount() - 1;

	private VanillaCommServer commServer;
	private boolean sequencerMode;
	private BlockingQueue<List<Serializable>> tomSendQueue = new LinkedBlockingQueue<List<Serializable>>();
	private boolean areAllServersReady = false;

	// MODIFIED:
	private static final int movingLatencyRange = 500;
	private Map<Integer, Queue<Long>> movingLatency = new HashMap<Integer, Queue<Long>>();
	public Map<Integer, Long> serverLatency = new HashMap<Integer, Long>();
	private Map<Integer, Long> sentSync = new HashMap<Integer, Long>();
	private BlockingQueue<SyncRequest> timeSyncQueue = new LinkedBlockingQueue<SyncRequest>();
	public boolean startSync = false;
	
	private long firstSpcArrivedTime = -1;

	public ConnectionMgr(int id) {
		sequencerMode = Elasql.serverId() == SEQUENCER_ID;
		commServer = new VanillaCommServer(id, this);
		new Thread(null, commServer, "VanillaComm-Server").start();

		// Only the sequencer needs to wait for all servers ready
		if (sequencerMode) {
			waitForServersReady();
			createTomSender();
		}
		createTimeSyncSender();
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		commServer.sendP2pMessage(ProcessType.CLIENT, clientId,
				new ClientResponse(clientId, rteId, txNum, rs));
	}
	
	public void sendStoredProcedureCall(boolean fromAppiaThread, int pid, Object[] pars) {
		commServer.sendTotalOrderMessage(new StoredProcedureCall(-1, -1, pid, pars));
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
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
				
				// Add to the total order request list
				tomRequest.add(spc);
			}
			
			// Send a total order request
			try {
				tomSendQueue.put(tomRequest);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (message.getClass().equals(TupleSet.class)) {
			TupleSet ts = (TupleSet) message;

			if (ts.sinkId() == MigrationSystemController.MSG_RANGE_FINISH) {
				Elasql.migraSysControl()
						.onReceiveMigrationRangeFinishMsg((MigrationRangeFinishMessage) ts.getMetadata());
				return;
			}

			// MODIFIED: Switch between with and without time interval
			// for (Tuple t : ts.getTupleSet()){
			// if (t.timestamp != null)
			// Elasql.remoteRecReceiver().cacheRemoteRecordWithTimeStamp(t);
			// else
			// Elasql.remoteRecReceiver().cacheRemoteRecord(t);
			// }
			for (Tuple t : ts.getTupleSet())
				Elasql.remoteRecReceiver().cacheRemoteRecord(t);

		} else if (message.getClass().equals(SyncRequest.class)) {
			// MODIFIED:
			SyncRequest sr = (SyncRequest) message;
			// System.out.printf("TimeSync Recv. From %d to %d \n", sr.getServerID(), Elasql.serverId());
			// Send other server's request back with current timestamp
			if (sr.isRequest()) {
				// sendServerTimeSync(ts.getServerID(), System.nanoTime() / 1000, false);
				try {
					SyncRequest temp = new SyncRequest(sr.getTimeSync().getServerID(),
							new TimeSync(System.nanoTime() / 1000, Elasql.serverId(), false));
					timeSyncQueue.put(temp);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return;
			} else {
				// MODIFIED:
				// Calculate latency, then push another request into timeSyncQueue
				long recvSync = sr.getTimeSync().getTime();
				long time_interval = (System.nanoTime() / 1000 - sentSync.get(sr.getTimeSync().getServerID())) / 2;
				long latency = sentSync.get(sr.getTimeSync().getServerID()) + time_interval - recvSync;
				// System.out.printf("A time_interval Value: %d, from Server%d to Server%d\n", time_interval, Elasql.serverId(), destID);
				// System.out.printf("A Server Latency Value: %d, from Server%d to Server%d\n", latency, Elasql.serverId(), destID);
				if (!movingLatency.containsKey(sr.getTimeSync().getServerID())) {
					movingLatency.put(sr.getTimeSync().getServerID(), new LinkedList<Long>());
					movingLatency.get(sr.getTimeSync().getServerID()).add(latency);
					serverLatency.put(sr.getTimeSync().getServerID(), latency);
				} else {
					movingLatency.get(sr.getTimeSync().getServerID()).add(latency);
					if (movingLatency.get(sr.getTimeSync().getServerID()).size() > movingLatencyRange)
						movingLatency.get(sr.getTimeSync().getServerID()).poll();
					long totalLatency = 0L;
					for (long lat : movingLatency.get(sr.getTimeSync().getServerID()))
						totalLatency += lat;
					long avgLatency = totalLatency / movingLatency.get(sr.getTimeSync().getServerID()).size();
					serverLatency.put(sr.getTimeSync().getServerID(), avgLatency);
				}

				sentSync.put(sr.getTimeSync().getServerID(), System.nanoTime() / 1000);
				// sendServerTimeSync(ts.getServerID(), sentSync.get(ts.getServerID()), true);
				try {
					SyncRequest temp = new SyncRequest(sr.getTimeSync().getServerID(),
							new TimeSync(System.nanoTime() / 1000, Elasql.serverId(), true));
					timeSyncQueue.put(temp);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		} else if (message instanceof MetricReport) {
			MetricReport report = (MetricReport) message;
			Elasql.performanceMgr().receiveMetricReport(report);
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		StoredProcedureCall spc = (StoredProcedureCall) message;
		spc.setTxNum(serialNumber);
		
		// Pass to the performance manager for monitoring the workload
		if (Elasql.performanceMgr() != null)
			Elasql.performanceMgr().monitorTransaction(spc);
		
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
		}).start();
		;
	}


	/**
		Called when ConnectionMgr is created, 
		this function add request to every other node into timeSyncQueue, 
		then create thread to keep sending request in the timeSyncQueue.
	 */
	private void createTimeSyncSender() {
		try {
			for (int i = 0; i < VanillaCommServer.getServerCount() - 1; i++) {
				SyncRequest temp = new SyncRequest(i, new TimeSync(System.nanoTime() / 1000, Elasql.serverId(), true));
				timeSyncQueue.put(temp);
				sentSync.put(i, System.nanoTime() / 1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(5);
						SyncRequest message = timeSyncQueue.take();
						commServer.sendP2pMessage(ProcessType.SERVER, message.getServerID(), message);
						// System.out.printf("TimeSync Sent. From %d to %d \n", Elasql.serverId(), message.getServerID());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		}).start();
		;
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
