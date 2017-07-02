package com.daoqidlv.filespilt.disruptor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class FileWriteThreadFactory implements ThreadFactory {
	
	private AtomicInteger threadCounter = new AtomicInteger(0);

	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setName("file-write-"+threadCounter.getAndIncrement());
		return t;
	}

}
