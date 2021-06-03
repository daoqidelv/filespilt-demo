package com.daoqidlv.filespilt.disruptor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.daoqidlv.filespilt.Constants;
import com.daoqidlv.filespilt.Master;
import com.daoqidlv.filespilt.Util;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 消费者生产者模式+Disruptor作为线程间数据交换的通道， 多线程读+多线程写+NIO， 大幅提升性能
 * 启动参数：D:\temp test.csv 10 PRODUCERCONSUMER 24 8 20480
 * @author Administrator
 *
 */
public class DisruptorMaster extends Master {
	
	private ExecutorService fileReadPool;
	
	private ExecutorService fileWritePool;
	
	private List<FileReadTask> fileReadTasks;
	
	private List<FileWriteTask> fileWriteTasks;
		
	private int readTaskNum;
	
	private int writeTaskNum;
	
	//每个FileWriteTask持有的队列长度。
	private int queueSize;
	//Disruptor的容量大小
	private int bufferSize;
	
	private Disruptor<FileLine> disruptor;

	public DisruptorMaster(String fileDir, String fileName, int subFileSizeLimit, int readTaskNum, int writeTaskNum, int queueSize, int bufferSize) {
		super(fileDir, fileName, subFileSizeLimit);
		this.readTaskNum = readTaskNum;
		this.writeTaskNum = writeTaskNum;
		this.queueSize = queueSize;
		this.bufferSize = bufferSize;
		this.fileReadPool = new ThreadPoolExecutor(this.readTaskNum, this.readTaskNum, 0l, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(200), new FileReadThreadFactory());
		this.fileWritePool = new ThreadPoolExecutor(this.writeTaskNum, this.writeTaskNum, 0l, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(200), new FileWriteThreadFactory());

		// Construct the Disruptor
		this.disruptor  = new Disruptor<FileLine>(
				new FileLineEventFactory(), 
        		this.bufferSize, 
        		new FileLineThreadFactory(), 
        		ProducerType.MULTI, 
        		new BlockingWaitStrategy()
        	);
	}
	
	public void init() {
		FileLineEventHandler fileLineEventHandler = new FileLineEventHandler();

		//allocate ReadTask
		fileReadTasks = initFileReadTask();		
		
		//allocate WriteTask
		fileWriteTasks = initFileWriteTask(fileLineEventHandler);
		
	    // Connect the handler
        disruptor.handleEventsWith(fileLineEventHandler);
				
	}
	

	
	@Override
	public void execute() {
		System.out.println("begin to spilt...");
		long startTime = System.currentTimeMillis();
		

		//submit FileWriteTasks
		List<Future> fileWriteFutureList = new ArrayList<Future>();
		for(FileWriteTask task : fileWriteTasks) {
			Future future = this.fileWritePool.submit(task, task);
			fileWriteFutureList.add(future);
		}
		
		//start disruptor
		disruptor.start();
		
		//submit FileReadTasks
		List<Future> fileReadFutureList = new ArrayList<Future>();
		for(FileReadTask task : fileReadTasks) {
			Future future = this.fileReadPool.submit(task, task);
			fileReadFutureList.add(future);
		}
		
		//get read result and shutdown fileReadPool
		int totalReadedSize = getTotalReadedSize(fileReadFutureList);
		this.fileReadPool.shutdown();	



		//判定是否可以停止fileWrite thread，前面已经停止了fileRead thread， 不会再产生新的任务，如果任务队列已经empty，则可以停止
		while (true) {
			boolean finished = true;
			for(FileWriteTask task : fileWriteTasks) {
				if(task.getQueue().isEmpty()) {
					finished = true;
					task.setDone(true);
				} else {
					finished = false;
					break;
				}
			} 
			
			if(finished) {
				break;
			}
			
		}
		//get write result and shutdown fileWritePool
		int totalWritenSize = getTotalWritenSize(fileWriteFutureList);
		this.fileWritePool.shutdown();
		

		//close disruptor
		this.disruptor.shutdown();
		
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
	/**
	 * 依据指定的readTaskNum初始化文件写入任务列表。
	 * @return List<FileWriteTask> —— 文件写入任务列表
	 */
	public List<FileWriteTask> initFileWriteTask(FileLineEventHandler fileLineEventHandler) {
		 if(this.writeTaskNum <= 0) {
			throw new IllegalArgumentException("文件写入任务数量必须大于0！");
		}

		List<FileWriteTask> taskList = new ArrayList<FileWriteTask>();
		for(int i=0; i<this.writeTaskNum; i++) {
			BlockingQueue<FileLine> queue = new LinkedBlockingQueue<FileLine>(this.queueSize);
//			BlockingQueue<FileLine> queue = new SynchronousQueue<FileLine>();
			FileWriteTask task = new FileWriteTask(i, this.getFileSpiltter(), queue);	
			fileLineEventHandler.addConsumerQueue(queue);
			taskList.add(task);
			System.out.println("创建一个FileWriteTask："+task);
		}
		return taskList;
	}
	
	/**
	 * 依据原始文件大小及读文件任务数，完成FileReadTask的初始化
	 * @return List<FileReadTask> FileReadTask任务列表
	 */
	public List<FileReadTask> initFileReadTask() {
		String orignFileFullName = Util.genFullFileName(this.getFileDir(),this.getFileName());
		if(this.readTaskNum <= 0) {
			throw new IllegalArgumentException("文件读取任务数量必须大于0！");
		}
		
		 RandomAccessFile orginFile = null;
		 int orignFileSize = 0;
		 try {
			orginFile = new RandomAccessFile(orignFileFullName, "r");
			//取得文件长度（字节数）  
			orignFileSize = (int)orginFile.length();
		} catch (FileNotFoundException e) {			
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		int avgToReadSize = orignFileSize/this.readTaskNum;
		
		List<FileReadTask> taskList = new ArrayList<FileReadTask>();
		
		int lastEndFilePointer = -1;
		int revisedEndFilePointer = 0;
		for(int i=0; i<this.readTaskNum; i++) {
			FileReadTask task = null;
			//最后一个任务将剩余未分配的数据也读取完成
			if(i == this.readTaskNum-1) {
				task = new FileReadTask(i, lastEndFilePointer+1, orignFileSize-1, orignFileFullName, this.disruptor);
			} else {
				revisedEndFilePointer = reviseEndFilePointer(lastEndFilePointer+avgToReadSize, orginFile);
				task = new FileReadTask(i, lastEndFilePointer+1, revisedEndFilePointer, orignFileFullName, this.disruptor);
				lastEndFilePointer = revisedEndFilePointer;				
			}
			taskList.add(task);
			System.out.println("创建一个FileReadTask："+task);
		}
		return taskList;
	}

	
	/**
	 * 修正任务的结束文件指针位置，加上第一个换行符/回车符 的偏移量，确保‘完整行’需求
	 * @param endFilePointer
	 * @param orginFile
	 * @return revisedEndFilePointer —— 修正后的endFilePointer
	 * TODO 此方案需要提前给定最大行大小，这个在实际情况时，很难有这个限定，待优化。
	 */
	private int reviseEndFilePointer(int endFilePointer, RandomAccessFile orginFile) {
		int revisedEndFilePointer = endFilePointer;
		byte[] tempBytes = new byte[1024];
		try {
			orginFile.seek(endFilePointer-1024+1);
			orginFile.readFully(tempBytes);  
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int revisedNum = 0;
		for(int i=tempBytes.length-1; i>=1; i--) {
			//换行符或者回车符，做修正
			if(Constants.ENTER_CHAR_ASCII == tempBytes[i-1] && Constants.NEW_LINE_CHAR_ASCII == tempBytes[i]) {
				break;
			} else {
				revisedNum++;
			}
		}
		return revisedEndFilePointer - revisedNum;
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
