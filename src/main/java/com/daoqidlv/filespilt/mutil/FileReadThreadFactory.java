package com.daoqidlv.filespilt.mutil;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class FileReadThreadFactory implements ThreadFactory {
	
	private AtomicInteger threadCounter = new AtomicInteger(0);

	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setName("file-read-"+threadCounter.getAndIncrement());
		return t;
	}

}
