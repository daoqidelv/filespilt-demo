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
	
	private final ExecutorService fileReadPool;		// 读线程池
	
	private final ExecutorService fileWritePool;	// 写线程池

	private final BlockingQueue<FileLine> queue; 	// 用于交换子文件内容的阻塞队列
	
	private final int readTaskNum;	// 读线程数
	
	private final int writeTaskNum;	// 写线程数
	
	private final int queueSize;	// 阻塞队列大小
	
	private TaskAllocater taskAllocater;

	// 构造函数，输入文件url，子文件大小（M）, 读写线程数, 阻塞队列大小
	public MutilThreadReadMaster(String fileDir, String fileName, int subFileSizeLimit, int readTaskNum, int writeTaskNum, int queueSize) {
		super(fileDir, fileName, subFileSizeLimit);
		this.readTaskNum = readTaskNum;
		this.writeTaskNum = writeTaskNum;
		this.queueSize = queueSize;
		this.fileReadPool = new ThreadPoolExecutor(this.readTaskNum, this.readTaskNum, 0l, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(200), new FileReadThreadFactory());
		this.fileWritePool = new ThreadPoolExecutor(this.writeTaskNum, this.writeTaskNum, 0l, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(200), new FileWriteThreadFactory());
		queue = new LinkedBlockingQueue<FileLine>(this.queueSize);
	}

	// 初始化TaskAllocator
	public void init() {
		//String orignFileFullName, int readTaskNum, int writeTaskNum, int maxLineSize, FileSpiltter fileSpiltter
		taskAllocater = new TaskAllocater(Util.genFullFileName(this.getFileDir(), this.getFileName()), this.readTaskNum, this.writeTaskNum, 1024, this.getFileSpiltter(), this.queue);
	}

	
	
	@Override
	// 在这里拆分文件初始化读写任务，提交任务到线程池，验证结果
	public void execute() {
		System.out.println("begin to spilt...");
		long startTime = System.currentTimeMillis();

		List<FileReadTask> fileReadTasks = this.taskAllocater.initFileReadTask();		  		// 把大文件分成多段，返回一个读任务List
		List<FileWriteTask> fileWriteTasks = this.taskAllocater.initFileWriteTask();			// 只是个普通的创建，返回写任务List, 自己有序号，都持有queue，从里面拿行来写自己文件

		// 提交读任务
		List<Future> fileReadFutureList = new ArrayList<Future>();
		for(FileReadTask task : fileReadTasks) {
			Future future = this.fileReadPool.submit(task, task);  		// 把Read任务提交到线程池，多出的任务会先等着，相当于降低读速度
			fileReadFutureList.add(future);
		}

		// 提交写任务
		List<Future> fileWriteFutureList = new ArrayList<Future>();
		for(FileWriteTask task : fileWriteTasks) {
			Future future = this.fileWritePool.submit(task, task);		// 写任务多出来的等着，相当于降低写速度
			fileWriteFutureList.add(future);
		}
		
		// 获取读结果，并在读结果全得到后（即读取任务全完成后），关闭读线程池
		int totalReadedSize = getTotalReadedSize(fileReadFutureList);
		this.fileReadPool.shutdown();

		// 检查消费队列，如果为空，说明写任务已经取完，没有要写的了
		while (true) {
			if (this.queue.isEmpty()) {
				FileWriteTask.setDone(true);  // 通过设置类静态标志位，通知所有的FileWriteTask停止任务处理
				break;
			} 
		}

		// 获取写结果，并关闭写线程池
		int totalWritenSize = getTotalWritenSize(fileWriteFutureList);
		this.fileWritePool.shutdown();

		// 检查源文件大小和最终写入文件大小和是否相等
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

	// 循环读Future读结果，如果有完成的，就读结果，并从结果列表移除，直到列表为空
	// todo：循环检测，不加sleep，会不会有性能问题？
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

	// 循环读Future写结果，如果有完成的，就读结果，并从结果列表移除，直到列表为空
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
