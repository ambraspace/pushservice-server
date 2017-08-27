package com.ambraspace.pushservice.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TimedFixedThreadPool extends ThreadPoolExecutor {
	
	
	private long maxRunTime;
	private TimeUnit timeUnit;
	
	private Map<Runnable, Timer> mapping;
	
	
	private class Timer extends Thread {
		
		private Thread observedThread = null;
		
		public Timer(Thread observedThread) {
			this.observedThread = observedThread;
			setName(observedThread.getName() + " (Timer)");
			setDaemon(true);
		}

		@Override
		public void run() {

			try {
				observedThread.join(timeUnit.toMillis(maxRunTime));
				observedThread.interrupt();
			} catch (InterruptedException e) {

			}
			
		}
		
	}

	
	public TimedFixedThreadPool(int size, long maxRunTime, TimeUnit timeUnit) {

		this(size, maxRunTime, timeUnit, Executors.defaultThreadFactory());

	}
	
	
	public TimedFixedThreadPool(int size, long maxRunTime, TimeUnit timeUnit, ThreadFactory factory) {

		super(
				size,
				size,
				0L,
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(),
				factory);
		
		this.maxRunTime = maxRunTime;
		this.timeUnit = timeUnit;
	
		this.mapping = new HashMap<Runnable, Timer>();
		
	}


	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		Timer timer = new Timer(t);
		mapping.put(r, timer);
		timer.start();
	}


	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		Timer timer = mapping.remove(r);
		timer.interrupt();
	}


}
