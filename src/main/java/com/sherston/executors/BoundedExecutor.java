package com.sherston.executors;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
/**
 * Bounded Executor
 * 
 * Makes it possible to block when a maximum number of submissions have been made
 * 
 * @see http://stackoverflow.com/questions/2001086/how-to-make-threadpoolexecutors-submit-method-block-if-it-is-saturated
 * @author pejot
 *
 */
public class BoundedExecutor {
	private final ExecutorService exec;
	private final Semaphore semaphore;

	public BoundedExecutor(ExecutorService exec, int bound) {
		this.exec = exec;
		this.semaphore = new Semaphore(bound);
	}

	public void submitTask(final Runnable command) throws InterruptedException,
			RejectedExecutionException {
		semaphore.acquire();
		try {
			exec.execute(new Runnable() {				
				public void run() {
					try {
						command.run();
					} finally {
						semaphore.release();
					}
				}
			});
		} catch (RejectedExecutionException e) {
			semaphore.release();
			throw e;
		}
	}
	
	public void shutdown(){
		exec.shutdown();
	}
}