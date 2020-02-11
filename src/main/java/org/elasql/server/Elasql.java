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
package org.elasql.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.cache.naive.NaiveCacheMgr;
import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationSystemController;
import org.elasql.migration.zephyr.AcceptByteSave;
import org.elasql.procedure.DdStoredProcedureFactory;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.procedure.naive.NaiveStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.server.ConnectionMgr;
import org.elasql.schedule.Scheduler;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.naive.NaiveScheduler;
import org.elasql.schedule.tpart.HeuristicNodeInserter;
import org.elasql.schedule.tpart.TGraph;
import org.elasql.schedule.tpart.TPartPartitioner;
import org.elasql.schedule.tpart.sink.CacheOptimizedSinker;
import org.elasql.storage.log.DdLogMgr;
import org.elasql.storage.metadata.HashPartitionPlan;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
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
	
	public static final long SYSTEM_INIT_TIME_MS = System.currentTimeMillis();
	
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
	private static MigrationMgr migraMgr;
	private static AcceptByteSave acceptByteSave;
	
	// Only for the sequencer
	private static MigrationSystemController migraSysControl;

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
		PartitionPlan partitionPlan = null;
		Class<?> planCls = ElasqlProperties.getLoader().getPropertyAsClass(
				Elasql.class.getName() + ".DEFAULT_PARTITION_PLAN", HashPartitionPlan.class,
				PartitionMetaMgr.class);

		try {
			partitionPlan = (PartitionPlan) planCls.newInstance();
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for partition manager");
			throw new RuntimeException();
		}
		
		init(dirName, id, isSequencer, factory, partitionPlan, null);
	}
	
	public static void init(String dirName, int id, boolean isSequencer, DdStoredProcedureFactory factory,
			PartitionPlan partitionPlan, MigrationComponentFactory migraComsFactory) {
		myNodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("ElaSQL initializing...");

		if (logger.isLoggable(Level.INFO))
			logger.info("using " + SERVICE_TYPE + " type service");

		if (isSequencer) {
			logger.info("initializing using Sequencer mode");
			initConnectionMgr(myNodeId, true);
			initPartitionMetaMgr(partitionPlan);
			if (migraComsFactory != null)
				migraSysControl = migraComsFactory.newSystemController();
			return;
		}

		// initialize core modules
		VanillaDb.init(dirName);

		// initialize DD modules
		initCacheMgr();
		initPartitionMetaMgr(partitionPlan);
		initScheduler(factory, migraComsFactory);
		initConnectionMgr(myNodeId, false);
		initDdLogMgr();
		if (migraComsFactory != null)
			migraMgr = migraComsFactory.newMigrationMgr();
		
		acceptByteSave = new AcceptByteSave();
		taskMgr().runTask(acceptByteSave);
		
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

	public static void initScheduler(DdStoredProcedureFactory factory, MigrationComponentFactory migraComsFactory) {
		switch (SERVICE_TYPE) {
		case NAIVE:
			if (!NaiveStoredProcedureFactory.class.isAssignableFrom(factory.getClass()))
				throw new IllegalArgumentException("The given factory is not a NaiveStoredProcedureFactory");
			scheduler = initNaiveScheduler((NaiveStoredProcedureFactory) factory);
			break;
		case CALVIN:
			if (!CalvinStoredProcedureFactory.class.isAssignableFrom(factory.getClass()))
				throw new IllegalArgumentException("The given factory is not a CalvinStoredProcedureFactory");
			CalvinStoredProcedureFactory calvinFactory = (CalvinStoredProcedureFactory) factory;
			if (migraComsFactory != null)
				calvinFactory = migraComsFactory.newMigrationSpFactory(calvinFactory);
			scheduler = initCalvinScheduler(calvinFactory);
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
		TPartPartitioner scheduler = new TPartPartitioner(factory, new HeuristicNodeInserter(),
				new CacheOptimizedSinker(), new TGraph());
		taskMgr().runTask(scheduler);
		return scheduler;
	}
	
	public static void initPartitionMetaMgr(PartitionPlan plan) {
		try {
			// Add a warper partition-meta-mgr for handling notifications
			// between servers
			plan = new NotificationPartitionPlan(plan);
			parMetaMgr = new PartitionMetaMgr(plan);
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
	
	public static AcceptByteSave CallAcceptByteSave() {
		return acceptByteSave;
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
	
	public static MigrationMgr migrationMgr() {
		return migraMgr;
	}
	
	public static MigrationSystemController migraSysControl() {
		return migraSysControl;
	}

	// ===============
	// Other Getters
	// ===============

	public static int serverId() {
		return myNodeId;
	}
}
