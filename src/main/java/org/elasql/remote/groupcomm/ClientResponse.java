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
package org.elasql.remote.groupcomm;

import java.io.Serializable;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;

/**
 * The commit message that server sends back to client after executing the
 * stored procedure call.
 * 
 */
public class ClientResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final int COMMITTED = 0, ROLLED_BACK = 1;

	private long txNum;

	private int clientId, rteId;

	private ElasqlSpResultSet result;

	public ClientResponse(int clientId, int rteId, long txNum, SpResultSet result) {
		this.txNum = txNum;
		this.clientId = clientId;
		this.rteId = rteId;
		this.result = new ElasqlSpResultSet(result);
	}

	public long getTxNum() {
		return txNum;
	}

	public ElasqlSpResultSet getResultSet() {
		return result;
	}

	public int getClientId() {
		return clientId;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public int getRteId() {
		return rteId;
	}

	public void setRteId(int rteId) {
		this.rteId = rteId;
	}
}
