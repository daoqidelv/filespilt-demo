package com.daoqidlv.filespilt.mutil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.daoqidlv.filespilt.Master;
import com.daoqidlv.filespilt.Util;

/**
 * 消费者生产者模式， 多线程读+多线程写+NIO， 大幅提升性能
 * 启动参数：D:\temp test.csv 10 PRODUCERCONSUMER 24 8 20480
 * @author Administrator
 *
 */
public class MutilThreadReadMaster extends Master {
	
	private ExecutorService fileReadPool;
	
	private ExecutorService fileWritePool;
	
	/**
	 * 用于交换子文件内容的阻塞队列
	 */
	private BlockingQueue<FileLine> queue;
	
	private int readTaskNum;
	
	private int writeTaskNum;
	
	private int queueSize;
	
	private TaskAllocater taskAllocater;

	public MutilThreadReadMaster(String fileDir, String fileName, int subFileSizeLimit, int readTaskNum, int writeTaskNum, int queueSize) {
		super(fileDir, fileName, subFileSizeLimit);
		this.readTaskNum = readTaskNum;
		this.writeTaskNum = writeTaskNum;
		this.queueSize = queueSize;
		this.fileReadPool = new ThreadPoolExecutor(this.readTaskNum, this.readTaskNum, 0l, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(200), new FileReadThreadFactory());
		this.fileWritePool = new ThreadPoolExecutor(this.writeTaskNum, this.writeTaskNum, 0l, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(200), new FileWriteThreadFactory());

		queue = new LinkedBlockingQueue<FileLine>(this.queueSize);
	}
	
	public void init() {
		//String orignFileFullName, int readTaskNum, int writeTaskNum, int maxLineSize, FileSpiltter fileSpiltter
		taskAllocater = new TaskAllocater(Util.genFullFileName(this.getFileDir(), this.getFileName()), this.readTaskNum, this.writeTaskNum, 1024, this.getFileSpiltter(), this.queue);
	}

	
	
	@Override
	public void excute() {
		System.out.println("begin to spilt...");
		long startTime = System.currentTimeMillis();
		//allocateReadTask
		List<FileReadTask> fileReadTasks = this.taskAllocater.initFileReadTask();		
		
		//allocatWriteTask
		List<FileWriteTask> fileWriteTasks = this.taskAllocater.initFileWriteTask();
		
		//submit FileReadTasks
		List<Future> fileReadFutureList = new ArrayList<Future>();
		for(FileReadTask task : fileReadTasks) {
			Future future = this.fileReadPool.submit(task, task);
			fileReadFutureList.add(future);
		}
		
		//submit FileWriteTasks
		List<Future> fileWriteFutureList = new ArrayList<Future>();
		for(FileWriteTask task : fileWriteTasks) {
			Future future = this.fileWritePool.submit(task, task);
			fileWriteFutureList.add(future);
		}
		
		//get read result and shutdown fileReadPool
		int totalReadedSize = getTotalReadedSize(fileReadFutureList);
		this.fileReadPool.shutdown();

		//判定是否可以停止fileWrite thread，前面已经停止了fileRead thread， 不会再产生新的任务，如果任务队列已经empty，则可以停止
		while (true) {
			if (this.queue.isEmpty()) {
				//通知所有的FileWriteTask停止任务处理
				FileWriteTask.setDone(true);
				break;
			} 
		}
		//get write result and shutdown fileWritePool
		int totalWritenSize = getTotalWritenSize(fileWriteFutureList);
		this.fileWritePool.shutdown();
		
		//check file spilt result
		//检查源文件大小和最终写入文件大小和是否相等。
		if(totalReadedSize == totalWritenSize) {
			System.out.println("文件拆分成功！源文件大小为："+totalReadedSize+", 拆分后的子文件大小之后为："+totalWritenSize);
		} else {
			System.out.println("文件拆分失败！源文件大小为："+totalReadedSize+", 拆分后的子文件大小之后为："+totalWritenSize);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("durition（ms）="+(endTime-startTime));	
		System.out.println("readTaskNum="+this.readTaskNum);
		System.out.println("writeTaskNum="+this.writeTaskNum);
		System.out.println("queueSize="+this.queueSize);
	}
	
	private int getTotalReadedSize(List<Future> fileReadFutureList) {
		Future futureTemp = null; 
		int totalReadedSize = 0;
		while(true) {
			for(Iterator<Future> it = fileReadFutureList.iterator(); it.hasNext();) {
				futureTemp = it.next();
				if(futureTemp.isDone()) {
					try {
						totalReadedSize += ((FileReadTask)futureTemp.get()).getReadedSize();
					} catch (InterruptedException e) {
						System.err.println("获取线程执行结果失败。");
						e.printStackTrace();
					} catch (ExecutionException e) {
						System.err.println("获取线程执行结果失败。");
						e.printStackTrace();
					}
					
					it.remove();
				}
			}
			if(fileReadFutureList == null || fileReadFutureList.size() == 0) {
				break;
			}
		}
		return totalReadedSize;
	}
	
	
	private int getTotalWritenSize(List<Future> fileWriteFutureList) {
		Future futureTemp = null; 
		int totalWritenSize = 0;
		while(true) {
			for(Iterator<Future> it = fileWriteFutureList.iterator(); it.hasNext();) {
				futureTemp = it.next();
				if(futureTemp.isDone()) {
					try {
						totalWritenSize += ((FileWriteTask)futureTemp.get()).getWritenSize();
					} catch (InterruptedException e) {
						System.err.println("获取线程执行结果失败。");
						e.printStackTrace();
					} catch (ExecutionException e) {
						System.err.println("获取线程执行结果失败。");
						e.printStackTrace();
					}
					
					it.remove();
				}
			}
			if(fileWriteFutureList == null || fileWriteFutureList.size() == 0) {
				break;
			}
		}
		return totalWritenSize;
	}
	
	

}
