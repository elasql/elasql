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
package org.elasql.remote.groupcomm;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 
 * This class defines a stored procedure call.
 * 
 */
public class StoredProcedureCall implements Serializable {
	
	private static BlockingQueue<Long> scheduleTimes = new LinkedBlockingQueue<Long>();
	
	static {
		new Thread(new Runnable() {

			@Override
			public void run() {
				long sum = 0;
				long count = 0;
				long startTime = System.currentTimeMillis();
				long lastReportTime = startTime;
				
				try {
					while (true) {
						long scheTime = scheduleTimes.take();
						scheTime /= 10000;
						sum += scheTime;
						count++;

						long currentTime = System.currentTimeMillis();
						if (currentTime - lastReportTime > 10000) {
							long average = sum / count;
							long clockTime = (currentTime - startTime) / 1000;
							String report = String.format("At %d, schedule: %d (count = %d)",
									clockTime, average, count);
							System.out.println(report);
							sum = 0;
							count = 0;
							lastReportTime = currentTime;
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}).start();
	}

	public static int PID_NO_OPERATION = Integer.MIN_VALUE;

	private static final long serialVersionUID = 8807383803517134106L;

	private Object[] objs;

	private long txNum = -1;
	private long startTime = -1;

	private int clientId, pid = PID_NO_OPERATION, connectionId = -1;

	public static StoredProcedureCall getNoOpStoredProcCall(int clienId) {
		return new StoredProcedureCall(clienId);
	}

	StoredProcedureCall(int clienId) {
		this.clientId = clienId;
	}

	public StoredProcedureCall(int clienId, int pid, Object... objs) {
		this.clientId = clienId;
		this.pid = pid;
		this.objs = objs;
	}

	public StoredProcedureCall(int clientId, int connId, int pid, Object... objs) {
		this.clientId = clientId;
		this.connectionId = connId;
		this.pid = pid;
		this.objs = objs;
	}

	public Object[] getPars() {
		return objs;
	}

	public long getTxNum() {
		return txNum;
	}

	public void setTxNum(long txNum) {
		this.txNum = txNum;
	}

	public int getClientId() {
		return clientId;
	}

	public int getConnectionId() {
		return connectionId;
	}

	public int getPid() {
		return pid;
	}

	public boolean isNoOpStoredProcCall() {
		return pid == PID_NO_OPERATION;
	}
	
	public void startRecordTime() {
		startTime = System.nanoTime();
	}
	
	public void stopRecordTime() {
		try {
			if (startTime != -1) {
				long time = System.nanoTime() - startTime;
				scheduleTimes.put(time);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
