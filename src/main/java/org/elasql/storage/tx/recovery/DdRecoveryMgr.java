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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class DdRecoveryMgr extends RecoveryMgr {

	private static BlockingQueue<StoredProcedureCall> spcLogQueue = new LinkedBlockingQueue<StoredProcedureCall>();

	private static final Object spcLoggerSyncObj = new Object();
	private static final Lock spcLoggerLock = new ReentrantLock();
	private static final Condition spcLoggerCondition = spcLoggerLock.newCondition();

	private static long lastLoggedTxn = -1;

	static {
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				while (true) {
					try {
						StoredProcedureCall spc = spcLogQueue.take();
						new StoredProcRequestRecord(spc.getTxNum(), spc.getClientId(), spc.getConnectionId(), spc.getPid(),
								spc.getPars()).writeToLog();
						// synchronized (spcLoggerSyncObj) {
						try {
							spcLoggerLock.lock();
							lastLoggedTxn = spc.getTxNum();
							// spcLoggerSyncObj.notifyAll();
							spcLoggerCondition.signalAll();
						} finally {
							spcLoggerLock.unlock();
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	public static void logRequest(StoredProcedureCall spc) {
		// TODO Commented for experiment
		spcLogQueue.add(spc);
	}

	public DdRecoveryMgr(long txNum) {
		super(txNum, true);
	}

	@Override
	public void onTxCommit(Transaction tx) {
		// TODO Commented for experiment
		// if (!tx.isReadOnly()) {
		// // synchronized (spcLoggerSyncObj) {
		// try {
		// spcLoggerLock.lock();
		// while (tx.getTransactionNumber() > lastLoggedTxn) {
		// try {
		// // spcLoggerSyncObj.wait();
		// spcLoggerCondition.await();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		// } finally {
		// spcLoggerLock.unlock();
		// }
		// }
	}
	// log sunk tx's remote readings
}
