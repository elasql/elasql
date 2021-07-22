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

public class SendableObjectSet implements Serializable {

	private static final long serialVersionUID = 1L;

	private long txNum;

	private int nodeId;

	private Object[] readings;

	public SendableObjectSet(int nodeId, long txNum, Object... readings) {
		this.txNum = txNum;
		this.nodeId = nodeId;
		this.readings = readings;
	}

	public long getTxNum() {
		return txNum;
	}

	public Object[] getReadSet() {
		return readings;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
}
