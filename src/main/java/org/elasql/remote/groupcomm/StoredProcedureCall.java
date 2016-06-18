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

/**
 * 
 * This class defines a stored procedure call.
 * 
 */
public class StoredProcedureCall implements Serializable {

	public static int PID_NO_OPERATION = Integer.MIN_VALUE;

	private static final long serialVersionUID = 8807383803517134106L;

	private Object[] objs;

	private long txNum = -1;

	private int clientId, pid = PID_NO_OPERATION, rteId = -1;

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

	public StoredProcedureCall(int clienId, int rteid, int pid, Object... objs) {
		this.clientId = clienId;
		this.rteId = rteid;
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

	public int getRteId() {
		return rteId;
	}

	public int getPid() {
		return pid;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public boolean isNoOpStoredProcCall() {
		return pid == PID_NO_OPERATION;
	}
}
