package org.elasql.util;

public class PeriodicalJob extends Thread {

	private final long startTime;
	private final long preiod;
	private final long totalTime;
	private final Runnable job;
	
	public PeriodicalJob(long preiod, long totalTime, Runnable job) {
		this(System.currentTimeMillis(), preiod, totalTime, job);
	}
	
	public PeriodicalJob(long startTime, long preiod, long totalTime, Runnable job) {
		this.startTime = startTime;
		this.preiod = preiod;
		this.totalTime = totalTime;
		this.job = job;
	}
	
	@Override
	public void run() {
		long currentTime = System.currentTimeMillis();
		long lastTime = startTime;
		
		try {
			while (currentTime - startTime < totalTime) {
				if (currentTime - lastTime > preiod) {
					job.run();
					lastTime =  (currentTime / preiod) * preiod;
				}
				
				if (preiod > 20)
					Thread.sleep(preiod / 20);
				else
					Thread.sleep(1);
				
				currentTime = System.currentTimeMillis();
			}
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}
}
