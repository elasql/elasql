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
package org.elasql.storage.log;

import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.storage.log.LogMgr;

/**
 * The low-level log manager. This log manager is responsible for writing log
 * records into a log file. A log record can be any sequence of integer and
 * string values. The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link org.vanilladb.core.storage.tx.recovery.RecoveryMgr recovery manager}.
 */
public class DdLogMgr extends LogMgr {

	public static final String DD_LOG_FILE;

	static {
		DD_LOG_FILE = ElasqlProperties.getLoader().getPropertyAsString(
				DdLogMgr.class.getName() + ".LOG_FILE", "vanilladddb.log");
	}

	public DdLogMgr() {
		super(DD_LOG_FILE);
	}
	
}
