package org.elasql.server.migration;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class MigrationPlan {
	
	private int sourcePart;
	private int destPart;
	// XXX: We should use RecordKeys, but this can save more bandwidth.
	private Set<Integer> keys;
	
	public MigrationPlan(int source, int dest) {
		sourcePart = source;
		destPart = dest;
		keys = new HashSet<Integer>();
	}
	
	public int getSourcePart() {
		return sourcePart;
	}
	
	public int getDestPart() {
		return destPart;
	}
	 
	public void addKey(Integer key) {
		keys.add(key);
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
		LinkedList<Integer> params = new LinkedList<Integer>();
		
		params.add(sourcePart);
		params.add(destPart);
		params.addAll(keys);
		
		return params.toArray(new Integer[0]);
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
