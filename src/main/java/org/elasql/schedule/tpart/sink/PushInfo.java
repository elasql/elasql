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
package org.elasql.schedule.tpart.sink;

import org.elasql.sql.RecordKey;

public class PushInfo {
	private long destTxNum;
	private int serverId;
	private RecordKey record;

	public PushInfo(long destTxNum, int serverId, RecordKey record) {
		this.destTxNum = destTxNum;
		this.serverId = serverId;
		this.record = record;
	}

	public long getDestTxNum() {
		return destTxNum;
	}

	public void setDestTxNum(long destTxNum) {
		this.destTxNum = destTxNum;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public RecordKey getRecord() {
		return record;
	}

	public void setRecord(RecordKey record) {
		this.record = record;
	}

	public String toString() {
		return "{" + record + ":" + serverId + ":" + destTxNum + "}";
	}
}
