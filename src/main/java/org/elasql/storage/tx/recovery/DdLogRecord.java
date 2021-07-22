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

import org.elasql.server.Elasql;
import org.elasql.storage.log.DdLogMgr;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public interface DdLogRecord extends LogRecord {
	/**
	 * @see LogRecord#op()
	 */
	static final int OP_SP_REQUEST = -99999;
	static DdLogMgr ddLogMgr = Elasql.DdLogMgr();

}
