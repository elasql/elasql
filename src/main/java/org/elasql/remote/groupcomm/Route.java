package org.elasql.remote.groupcomm;

import java.io.Serializable;

public class Route implements Serializable {
	
	private int destination;
	
	public Route(int destination) {
		this.destination = destination;
	}
	
	public int getDestination() {
		return destination;
	}
}
