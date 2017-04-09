package org.vanilladb.dd.schedule.tpart;

import org.vanilladb.dd.junk.TestTPartSP;

public class TPartStoredProcedureFactory {
	// XXX : Test if this is needed
	public TPartStoredProcedure getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure sp;
		switch (pid) {
		case 999:
			sp = new TestTPartSP(txNum);
			break;
		default:
			throw new UnsupportedOperationException();

		}
		return sp;
	}
}
