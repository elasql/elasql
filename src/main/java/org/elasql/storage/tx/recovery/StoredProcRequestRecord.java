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
package org.elasql.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public class StoredProcRequestRecord implements DdLogRecord {
	private long txNum;
	private int clientId, connectionId, procedureId;
	private Object[] pars;
	private LogSeqNum lsn;
	
	/**
	 * 
	 * Creates a new stored procedure request log record for the specified
	 * transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param cid
	 * @param pid
	 * @param pars
	 */
	public StoredProcRequestRecord(long txNum, int cid, int connId, int pid,
			Object... pars) {
		this.txNum = txNum;
		this.clientId = cid;
		this.connectionId = connId;
		this.procedureId = pid;
		this.pars = pars;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public StoredProcRequestRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		this.clientId = (Integer) rec.nextVal(INTEGER).asJavaVal();
		this.connectionId = (Integer) rec.nextVal(INTEGER).asJavaVal();
		this.procedureId = (Integer) rec.nextVal(INTEGER).asJavaVal();

		// FIXME
		// See writeToLog()
		this.pars = new Object[(Integer) rec.nextVal(INTEGER).asJavaVal()];
		
		lsn = rec.getLSN();
	}
	
	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return ddLogMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_SP_REQUEST;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {
		// do nothing
	}

	@Override
	public void redo(Transaction tx) {
		// TODO replay the stored procedure
	}

	@Override
	public String toString() {
		return "<SP_REQUEST " + txNum + " " + procedureId + " " + clientId + 
				" " + Arrays.toString(pars) + " >";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new IntegerConstant(clientId));
		rec.add(new IntegerConstant(connectionId));
		rec.add(new IntegerConstant(procedureId));
		// XXX: we only record the size of the parameters for now
		rec.add(new IntegerConstant(pars.length));
		// Real code
//		rec.add(new VarcharConstant(Arrays.toString(pars)));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}
}
