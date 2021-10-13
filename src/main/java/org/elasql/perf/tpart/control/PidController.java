package org.elasql.perf.tpart.control;

import org.elasql.util.ElasqlProperties;

public class PidController {
	
	private static final double K_P;
	private static final double K_I;
	private static final double K_D;
	
	static {
		K_P = ElasqlProperties.getLoader().getPropertyAsDouble(
				PidController.class.getName() + ".K_P", 1.0);
		K_I = ElasqlProperties.getLoader().getPropertyAsDouble(
				PidController.class.getName() + ".K_I", 0.0);
		K_D = ElasqlProperties.getLoader().getPropertyAsDouble(
				PidController.class.getName() + ".K_D", 0.0);
	}
	
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
	
	public void updateControlParameters(double timeOffsetInSecs) {
		double error = reference - observation;
		
		// Calculate PID values
		double proportional = error;
		integral += error * timeOffsetInSecs;
		double derivative = (error - previousError) / timeOffsetInSecs;
		
		// Calculate control signal
		double controlSignal = K_P * proportional +
				K_I * integral + K_D * derivative;
		
		// Update the parameter
		controlParameter = initialParameter * Math.exp(-controlSignal);
		
		System.out.println(String.format("[%f, %f, %f, %f, %f, %f, %f, %f]",
				reference, observation, error, proportional, integral, derivative, controlSignal, controlParameter));
		
		// Record the error
		previousError = error;
	}
	
	public double getControlParameter() {
		return controlParameter;
	}
}
