package org.elasql.migration.zephyr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.migration.mgcrab.MgCrabStoredProcFactory;
import org.elasql.remote.groupcomm.ByteSet;
import org.elasql.remote.groupcomm.Bytes;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class AcceptByteSave extends Task{

	private static Logger logger = Logger.getLogger(AcceptByteSave.class.getName());
	private BlockingQueue<Bytes> eventQueue;
	private Map<String, ByteSet> datamap;
	private Map<String, IndexInfo> allIndexes;
	private Map<String, SearchKeyType> allSKtype;
	private int indexCount;

	public AcceptByteSave() {
		eventQueue = new LinkedBlockingQueue<Bytes>();
		datamap = new HashMap<String, ByteSet>();
		allIndexes = new HashMap<String, IndexInfo>();
		allSKtype = new HashMap<String, SearchKeyType>();
		
	}
		
	@Override
	public void run() {

		while (true) {
			try {
				Bytes data = eventQueue.take();
//				if (logger.isLoggable(Level.INFO)) {
//					logger.info(String.format("serial_nm: %s, part: %d, total : %d, "
//							+ "byte_lenth: %d", data.serial_nm, data.part, data.totalPart, data.byte_lenth));
//				}
				
				if(data.serial_nm.equals("startMigrate")) {
					indexCount = data.totalPart;
					continue;
				}
				
				if(datamap.containsKey(data.serial_nm)) {
					datamap.get(data.serial_nm).addByte(data);
				} else {
					ByteSet b = new ByteSet();
					b.addByte(data);
					datamap.put(data.serial_nm, b);
				}
				
				if(data.part == 0) {
					allIndexes.put(data.serial_nm, data.ii);
					allSKtype.put(data.serial_nm, data.sktype);
				}
				
				/*if (logger.isLoggable(Level.INFO)) {
					logger.info(String.format("byteSet size %d", datamap.get(data.serial_nm).size()));
				}*/
				if(data.totalPart == datamap.get(data.serial_nm).size()) {
					if (logger.isLoggable(Level.INFO)) {
						logger.info(String.format("ready save file"));
					}
					FileOutputStream fop = null;
					File file;
					try {
						file = VanillaDb.fileMgr().getFile(data.serial_nm);
						fop = new FileOutputStream(file);
						datamap.get(data.serial_nm).sort();
						for(Bytes b: datamap.get(data.serial_nm).getByteSet()) {
//							if (logger.isLoggable(Level.INFO)) {
//								logger.info(String.format("save file: serial_nm: %s, part: %d, total : %d, "
//										+ "byte_lenth: %d", b.serial_nm, b.part, b.totalPart, b.byte_lenth));
//							}
							
							fop.write(b.rec, 0, b.byte_lenth);
							//fop.flush();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}finally{
						try {
							if(fop != null) {
								fop.close();
							}
							datamap.remove(data.serial_nm);
							indexCount = indexCount - 1;
							if(indexCount == 0) {
//								if (logger.isLoggable(Level.INFO)) {
//									logger.info(String.format("ready send dual start"));
//								}
								Object[] params = new Object[] {};
								Object[] call = { new StoredProcedureCall(-1, -1, 
										ZephyrStoredProcFactory.SP_DUAL_START, params)};
								Elasql.connectionMgr().sendBroadcastRequest(call, false);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

// ======================
// APIs for Other Threads
// ======================
		
	public void byteAcceptAndSave(Bytes data) {
		eventQueue.add(data);
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("in the byteAcceptAndSave"));
		}
	}
	
	public List<IndexInfo> getIndexInfo() {
		return allIndexes.values().stream()
				.collect(Collectors.toList());
	}
	
	public List<SearchKeyType> getSearchKeyType() {
		return allSKtype.values().stream()
				.collect(Collectors.toList());
	}
}

