package org.elasql.perf.tpart.control;

public class PidController {
	
	private static final double K_P = 1.0;
	private static final double K_I = 1.0;
	private static final double K_D = 1.0;
	
	private double initialParameter;
	private double controlParameter;
	private double observation;
	private double reference;
	
	// Intermediate states
	private double previousError;
	private double integral;
	
	public PidController(double initialParameter) {
		this.initialParameter = initialParameter;
		this.controlParameter = initialParameter;
	}
	
	public void setObservation(double observation) {
		this.observation = observation;
	}
	
	public double getObservation() {
		return observation;
	}
	
	public void setReference(double reference) {
		this.reference = reference;
	}
	
	public void updateControlParameters(double timeOffset) {
		double error = reference - observation;
		
		// Calculate PID values
		double proportional = error;
		integral += error * timeOffset;
		double derivative = (error - previousError) / timeOffset;
		
		// Calculate control signal
		double controlSignal = K_P * proportional +
				K_I * integral + K_D * derivative;
		
		// Update the parameter
		controlParameter = initialParameter * Math.exp(-controlSignal);
		
		// Record the error
		previousError = error;
	}
	
	public double getControlParameter() {
		return controlParameter;
	}
}
