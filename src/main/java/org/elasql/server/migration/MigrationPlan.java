package org.elasql.server.migration;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.elasql.sql.RecordKey;

public class MigrationPlan {
	
	private int sourcePart;
	private int destPart;
	private Set<RecordKey> keys;
	
	public MigrationPlan(int source, int dest) {
		sourcePart = source;
		destPart = dest;
		keys = new HashSet<RecordKey>();
	}
	
	public int getSourcePart() {
		return sourcePart;
	}
	
	public int getDestPart() {
		return destPart;
	}
	 
	public void addKey(RecordKey key) {
		keys.add(key);
	}
	 
	public Set<RecordKey> getKeys() {
		return keys;
	}
	
	public void mergePlan(MigrationPlan plan) {
		if (sourcePart != plan.sourcePart ||
				destPart != plan.destPart)
			throw new RuntimeException("The plan does not match.");
		
		keys.addAll(plan.keys);
	}
	
	public int keyCount() {
		return keys.size();
	}
	
	public Object[] toStoredProcedureRequest() {
		LinkedList<Object> params = new LinkedList<Object>();
		
		params.add(sourcePart);
		params.add(destPart);
		params.addAll(keys);
		
		return params.toArray(new Object[0]);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("{From: part.");
		sb.append(sourcePart);
		sb.append(" to part.");
		sb.append(destPart);
		sb.append(" ");
		sb.append(keys);
		sb.append("}");
		
		return sb.toString();
	}
}
