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
package org.elasql.procedure.naive;

import org.elasql.procedure.StoredProcedureTask;
import org.elasql.server.Elasql;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class NaiveStoredProcedureTask extends StoredProcedureTask<NaiveStoredProcedure<?>> {
	
	public NaiveStoredProcedureTask(int cid, int connId, long txNum,
			NaiveStoredProcedure<?> sp) {
		super(cid, connId, txNum, sp);
	}

	public void run() {
		SpResultSet rs = sp.execute();
		Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);
	}
	
	public void lockConservatively() {
		sp.requestConservativeLocks();
	}
}
