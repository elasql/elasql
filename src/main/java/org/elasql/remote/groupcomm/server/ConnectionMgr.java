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

import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.ByteSet;
import org.elasql.remote.groupcomm.Bytes;
import org.elasql.remote.groupcomm.ClientResponse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.comm.messages.TotalOrderMessage;
import org.vanilladb.comm.server.ServerAppl;
import org.vanilladb.comm.server.ServerNodeFailListener;
import org.vanilladb.comm.server.ServerP2pMessageListener;
import org.vanilladb.comm.server.ServerTotalOrderedMessageListener;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class ConnectionMgr
		implements ServerTotalOrderedMessageListener, ServerP2pMessageListener, ServerNodeFailListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class.getName());

	private ServerAppl serverAppl;
	private int myId;
	private boolean sequencerMode;
	private BlockingQueue<TotalOrderMessage> tomQueue = new LinkedBlockingQueue<TotalOrderMessage>();
	
	public ConnectionMgr(int id, boolean seqMode) {
		myId = id;
		sequencerMode = seqMode;
		serverAppl = new ServerAppl(id, this, this, this);
		serverAppl.start();

		// wait for all servers to start up
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		serverAppl.startPFD();

		if (!sequencerMode) {
			VanillaDb.taskMgr().runTask(new Task() {

				@Override
				public void run() {
					while (true) {
						try {
							TotalOrderMessage tom = tomQueue.take();
							for (int i = 0; i < tom.getMessages().length; ++i) {
								StoredProcedureCall spc = (StoredProcedureCall) tom.getMessages()[i];
								spc.setTxNum(tom.getTotalOrderIdStart() + i);
								Elasql.scheduler().schedule(spc);
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

				}
			});
		}
	}

	public void sendClientResponse(int clientId, int rteId, long txNum, SpResultSet rs) {
		// call the communication module to send the response back to client
		P2pMessage p2pmsg = new P2pMessage(new ClientResponse(clientId, rteId, txNum, rs), clientId,
				ChannelType.CLIENT);
		serverAppl.sendP2pMessage(p2pmsg);
	}

	public void sendBroadcastRequest(Object[] objs, boolean isAppiaThread) {
		serverAppl.sendBroadcastRequest(objs, isAppiaThread);
	}

	public void callStoredProc(int pid, Object... pars) {
		StoredProcedureCall[] spcs = { new StoredProcedureCall(myId, pid, pars) };
		serverAppl.sendTotalOrderRequest(spcs);
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		P2pMessage p2pmsg = new P2pMessage(reading, nodeId, ChannelType.SERVER);
		serverAppl.sendP2pMessage(p2pmsg);
	}
	
	public void pushByteSet(int nodeId, ByteSet reading) {
		P2pMessage p2pmsg = new P2pMessage(reading, nodeId, ChannelType.SERVER);
		serverAppl.sendP2pMessage(p2pmsg);
	}

	@Override
	public void onRecvServerP2pMessage(P2pMessage p2pmsg) {
		Object msg = p2pmsg.getMessage();
		if (msg.getClass().equals(TupleSet.class)) {
			TupleSet ts = (TupleSet) msg;
			
			if (ts.sinkId() == MigrationSystemController.MSG_RANGE_FINISH) {
				Elasql.migraSysControl().onReceiveMigrationRangeFinishMsg(
						(MigrationRangeFinishMessage) ts.getMetadata());
				return;
			}
			
			for (Tuple t : ts.getTupleSet())
				Elasql.remoteRecReceiver().cacheRemoteRecord(t);
		} else if(msg.getClass().equals(ByteSet.class)) {
			if (logger.isLoggable(Level.INFO))
				logger.info("get ByteSet, ready to call byteAcceptAndSave");
			
			ByteSet bs = (ByteSet) msg;
			
			for (Bytes b : bs.getByteSet()) {
				/*if (logger.isLoggable(Level.INFO)) {
					logger.info(String.format("serial_nm: %s, part: %d, total : %d, "
							+ "byte_lenth: %d", b.serial_nm, b.part, b.totalPart, b.byte_lenth));
				}*/
				Elasql.CallAcceptByteSave().byteAcceptAndSave(b);
			}
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public void onRecvServerTotalOrderedMessage(TotalOrderMessage tom) {
		if (sequencerMode)
			return;

		try {
			tomQueue.put(tom);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onNodeFail(int id, ChannelType ct) {
		// do nothing
	}

	@Override
	public String mkClientResponse(Object o) {
		return null;
	}
	
	public SocketAddress getSocketAddress(int nodeId){
		return serverAppl.getSocketAddress(nodeId);
	}
}
