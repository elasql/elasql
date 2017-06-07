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
package org.elasql.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.cache.naive.NaiveCacheMgr;
import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.procedure.DdStoredProcedureFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.procedure.naive.NaiveStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.server.ConnectionMgr;
import org.elasql.schedule.Scheduler;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.naive.NaiveScheduler;
import org.elasql.schedule.tpart.HeuristicNodeInserter;
import org.elasql.schedule.tpart.SupaTGraph;
import org.elasql.schedule.tpart.TPartPartitioner;
import org.elasql.schedule.tpart.sink.CacheOptimizedSinker;
import org.elasql.storage.log.DdLogMgr;
import org.elasql.storage.metadata.HashBasedPartitionMetaMgr;
import org.elasql.storage.metadata.NotificationPartMetaMgr;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;

public class Elasql extends VanillaDb {
	private static Logger logger = Logger.getLogger(VanillaDb.class.getName());

	public static final long START_TX_NUMBER = 0;

	/**
	 * The type of transactional execution engine supported by distributed
	 * deterministic VanillaDB.
	 */
	public enum ServiceType {
		NAIVE, CALVIN, TPART;

		static ServiceType fromInteger(int index) {
			switch (index) {
			case 0:
				return NAIVE;
			case 1:
				return CALVIN;
			case 2:
				return TPART;
			default:
				throw new RuntimeException("Unsupport service type");
			}
		}
	}
	
	public static final ServiceType SERVICE_TYPE;
	
	static {
		// read service type properties
		int type = ElasqlProperties.getLoader().getPropertyAsInteger(Elasql.class.getName() + ".SERVICE_TYPE",
				ServiceType.NAIVE.ordinal());
		SERVICE_TYPE = ServiceType.fromInteger(type);
	}

	// DD modules
	private static ConnectionMgr connMgr;
	private static PartitionMetaMgr parMetaMgr;
	private static RemoteRecordReceiver remoteRecReceiver;
	private static Scheduler scheduler;
	private static DdLogMgr ddLogMgr;

	// connection information
	private static int myNodeId;

	/**
	 * Initializes the system. This method is called during system startup. For
	 * sequencers, it can set {@code initVanillaDb} as {@code false} to avoid
	 * initializing underlying databases.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 * @param id
	 *            the id of the server
	 * @param isSequencer
	 *            is this server a sequencer
	 */
	public static void init(String dirName, int id, boolean isSequencer, DdStoredProcedureFactory factory) {
		PartitionMetaMgr partMetaMgr = null;
		Class<?> parMgrCls = ElasqlProperties.getLoader().getPropertyAsClass(
				Elasql.class.getName() + ".DEFAULT_PARTITION_META_MGR", HashBasedPartitionMetaMgr.class,
				PartitionMetaMgr.class);

		try {
			partMetaMgr = (PartitionMetaMgr) parMgrCls.newInstance();
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for partition manager");
			throw new RuntimeException();
		}

		init(dirName, id, isSequencer, factory, partMetaMgr);
	}

	public static void init(String dirName, int id, boolean isSequencer, DdStoredProcedureFactory factory,
			PartitionMetaMgr partitionMetaMgr) {
		myNodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("ElaSQL initializing...");

		if (logger.isLoggable(Level.INFO))
			logger.info("using " + SERVICE_TYPE + " type service");

		if (isSequencer) {
			logger.info("initializing using Sequencer mode");
			initConnectionMgr(myNodeId, true);
			return;
		}

		// initialize core modules
		VanillaDb.init(dirName);

		// initialize DD modules
		initCacheMgr();
		initPartitionMetaMgr(partitionMetaMgr);
		initScheduler(factory);
		initConnectionMgr(myNodeId, false);
		initDdLogMgr();
	}

	// ================
	// Initializers
	// ================

	public static void initCacheMgr() {
		switch (SERVICE_TYPE) {
		case NAIVE:
			remoteRecReceiver = new NaiveCacheMgr();
			break;
		case CALVIN:
			remoteRecReceiver = new CalvinPostOffice();
			break;
		case TPART:
			remoteRecReceiver = new TPartCacheMgr();
			break;

		default:
			throw new UnsupportedOperationException();
		}
	}

	public static void initScheduler(DdStoredProcedureFactory factory) {
		switch (SERVICE_TYPE) {
		case NAIVE:
			if (!NaiveStoredProcedureFactory.class.isAssignableFrom(factory.getClass()))
				throw new IllegalArgumentException("The given factory is not a NaiveStoredProcedureFactory");
			scheduler = initNaiveScheduler((NaiveStoredProcedureFactory) factory);
			break;
		case CALVIN:
			if (!CalvinStoredProcedureFactory.class.isAssignableFrom(factory.getClass()))
				throw new IllegalArgumentException("The given factory is not a CalvinStoredProcedureFactory");
			scheduler = initCalvinScheduler((CalvinStoredProcedureFactory) factory);
			break;
		case TPART:
			if (!TPartStoredProcedureFactory.class.isAssignableFrom(factory.getClass()))
				throw new IllegalArgumentException("The given factory is not a TPartStoredProcedureFactory");
			scheduler = initTPartScheduler((TPartStoredProcedureFactory) factory);
			break;
		default:
			throw new UnsupportedOperationException();
		}
	}

	public static Scheduler initNaiveScheduler(NaiveStoredProcedureFactory factory) {
		NaiveScheduler scheduler = new NaiveScheduler(factory);
		taskMgr().runTask(scheduler);
		return scheduler;
	}

	public static Scheduler initCalvinScheduler(CalvinStoredProcedureFactory factory) {
		CalvinScheduler scheduler = new CalvinScheduler(factory);
		taskMgr().runTask(scheduler);
		return scheduler;
	}

	public static Scheduler initTPartScheduler(TPartStoredProcedureFactory factory) {
		TPartPartitioner scheduler = new TPartPartitioner(factory,  new HeuristicNodeInserter(),
				new CacheOptimizedSinker(), new SupaTGraph());

		taskMgr().runTask(scheduler);
		return scheduler;
	}

	public static void initPartitionMetaMgr(PartitionMetaMgr partitionMetaMgr) {
		try {
			// Add a warper partition-meta-mgr for handling notifications
			// between servers
			parMetaMgr = new NotificationPartMetaMgr(partitionMetaMgr);
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for partition manager");
			throw new RuntimeException();
		}
	}

	public static void initConnectionMgr(int id, boolean isSequencer) {
		connMgr = new ConnectionMgr(id, isSequencer);
	}

	public static void initDdLogMgr() {
		ddLogMgr = new DdLogMgr();
	}

	// ================
	// Module Getters
	// ================

	public static RemoteRecordReceiver remoteRecReceiver() {
		return remoteRecReceiver;
	}

	public static Scheduler scheduler() {
		return scheduler;
	}

	public static PartitionMetaMgr partitionMetaMgr() {
		return parMetaMgr;
	}

	public static ConnectionMgr connectionMgr() {
		return connMgr;
	}

	public static DdLogMgr DdLogMgr() {
		return ddLogMgr;
	}

	// ===============
	// Other Getters
	// ===============

	public static int serverId() {
		return myNodeId;
	}
}
