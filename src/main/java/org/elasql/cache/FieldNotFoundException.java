package org.elasql.cache;

public class FieldNotFoundException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;

	public FieldNotFoundException() {
		super();
	}
	
	public FieldNotFoundException(String message) {
		super(message);
	}
}
