package com.daoqidlv.filespilt.disruptor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class FileLineThreadFactory implements ThreadFactory {
	
	private AtomicInteger threadCounter = new AtomicInteger(0);

	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setName("file-line-"+threadCounter.getAndIncrement());
		return t;
	}

}
