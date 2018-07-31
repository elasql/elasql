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
package org.elasql.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The CURD interfaces to VanillaCore.
 */
public class VanillaCoreCrud {

	public static CachedRecord read(RecordKey key, Transaction tx) {
		// Open index select scan
		TablePlan tp = new TablePlan(key.getTableName(), tx);
		Schema sch = tp.schema();
		Map<String, IndexInfo> indexInfoMap = Elasql.catalogMgr()
				.getIndexInfo(key.getTableName(), tx);
		Plan p = tp;
		for (String fld : key.getKeyFldSet()) {
			IndexInfo ii = indexInfoMap.get(fld);
			if (ii != null) {
				p = new IndexSelectPlan(tp, ii, ConstantRange.newInstance(key
						.getKeyVal(fld)), tx);
				break;
			}
		}

		p = new SelectPlan(p, key.getPredicate());
		SelectScan s = (SelectScan) p.open();
		s.beforeFirst();
		CachedRecord rec = null;

		if (s.next()) {
			Map<String, Constant> fldVals = new HashMap<String, Constant>();
			for (String fld : sch.fields())
				fldVals.put(fld, s.getVal(fld));
			rec = new CachedRecord(fldVals);
		}
		s.close();

		return rec;
	}

	// True: Found match record, False: Cannot find match record
	public static boolean update(RecordKey key, CachedRecord rec, Transaction tx) {
		TablePlan tp = new TablePlan(key.getTableName(), tx);
		Map<String, IndexInfo> indexInfoMap = Elasql.catalogMgr()
				.getIndexInfo(key.getTableName(), tx);

		// get target fields
		Collection<String> targetflds = rec.getDirtyFldNames();

		// XXX: Remove search key from target fields if they existed
		// for Constraint C1 below
		for (String searchFld : key.getKeyFldSet())
			targetflds.remove(searchFld);

		// open all indexes associate with target fields
		HashMap<String, Index> targetIdxMap = new HashMap<String, Index>();
		for (String fld : targetflds) {
			IndexInfo ii = indexInfoMap.get(fld);
			Index idx = (ii == null) ? null : ii.open(tx);
			if (idx != null)
				targetIdxMap.put(fld, idx);
		}

		// create a IndexSelectPlan
		Plan selectPlan = null;
		for (String fld : indexInfoMap.keySet()) {
			ConstantRange cr = key.getPredicate().constantRange(fld);
			if (cr != null && !targetflds.contains(fld)) { // Constraint C1
				IndexInfo ii = indexInfoMap.get(fld);
				selectPlan = new IndexSelectPlan(tp, ii, cr, tx);
				break;
			}
		}
		if (selectPlan == null)
			selectPlan = new SelectPlan(tp, key.getPredicate());
		else
			selectPlan = new SelectPlan(selectPlan, key.getPredicate());
		UpdateScan s = (UpdateScan) selectPlan.open();
		s.beforeFirst();

		// the record key should identifies one record uniquely
		if (s.next()) {
			Constant newval, oldval;
			for (String fld : targetflds) {
				newval = rec.getVal(fld);
				oldval = s.getVal(fld);
				if (newval.equals(oldval))
					continue;
				// update the appropriate index, if it exists
				Index idx = targetIdxMap.get(fld);
				if (idx != null) {
					RecordId rid = s.getRecordId();
					idx.delete(oldval, rid, true);
					idx.insert(newval, rid, true);
				}
				s.setVal(fld, newval);
			}
		} else
			return false;
		
		// close opened indexes
		for (String fld : targetflds) {
			Index idx = targetIdxMap.get(fld);
			if (idx != null)
				idx.close();
		}
		s.close();

		// XXX: Do we need this ?
		// VanillaDdDb.statMgr().countRecordUpdates(tblname, 1);
		
		return true;
	}

	public static void insert(RecordKey key, CachedRecord rec, Transaction tx) {
		String tblname = key.getTableName();
		Plan p = new TablePlan(tblname, tx);
		Map<String, IndexInfo> indexes = Elasql.catalogMgr().getIndexInfo(
				tblname, tx);
		Map<String, Constant> m = rec.getFldValMap();

		// first, insert the record
		UpdateScan s = (UpdateScan) p.open();
		s.insert();
		RecordId rid = s.getRecordId();

		// then modify each field, inserting an index record if appropriate
		for (Entry<String, Constant> e : m.entrySet()) {
			Constant val = e.getValue();
			if (val == null)
				continue;
			// first, insert into index
			IndexInfo ii = indexes.get(e.getKey());
			if (ii != null) {
				Index idx = ii.open(tx);
				idx.insert(val, rid, true);
				idx.close();
			}
			// insert into record file
			s.setVal(e.getKey(), val);
		}
		s.close();

		// XXX: Do we need this ?
		// VanillaDdDb.statMgr().countRecordUpdates(tblname, 1);
	}

	public static void delete(RecordKey key, Transaction tx) {
		String tblname = key.getTableName();
		TablePlan tp = new TablePlan(tblname, tx);
		Map<String, IndexInfo> indexInfoMap = Elasql.catalogMgr()
				.getIndexInfo(tblname, tx);

		Plan p = tp;
		for (String fld : key.getKeyFldSet()) {
			IndexInfo ii = indexInfoMap.get(fld);
			if (ii != null) {
				p = new IndexSelectPlan(tp, ii, ConstantRange.newInstance(key
						.getKeyVal(fld)), tx);
				break;
			}
		}
		p = new SelectPlan(p, key.getPredicate());
		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		// the record key should identifies one record uniquely
		if (s.next()) {
			RecordId rid = s.getRecordId();
			// delete the record from every index
			for (String fldname : indexInfoMap.keySet()) {
				Constant val = s.getVal(fldname);
				Index idx = indexInfoMap.get(fldname).open(tx);
				idx.delete(val, rid, true);
				idx.close();
			}
			s.delete();
		}
		s.close();

		// XXX: Do we need this ?
		// VanillaDdDb.statMgr().countRecordUpdates(tblname, 1);
	}
}
