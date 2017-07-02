package com.daoqidlv.filespilt.disruptor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.daoqidlv.filespilt.FileSpiltter;

/**
 * 文件写入任务。
 * 将文件内容写入到子文件中，写入时完成子文件自动生成和切换。
 * 子文件一次性写入，使用NIO写入
 * @author Administrator
 *
 */
public class FileWriteTask extends Thread {
	/**
	 * 任务序号
	 */
	private int taskSeq;
	/**
	 * 任务名称
	 */
	private String taskName;
	/**
	 * 文件切割器
	 */
	private FileSpiltter fileSpiltter;
	/**
	 * 实际写入的数据总大小
	 */
	private int writenSize;
	/**
	 * 用于交换子文件内容的阻塞队列
	 */
	private BlockingQueue<FileLine> queue;
	/**
	 * 子文件 数量计数器
	 */
	private static AtomicInteger subFileCounter =  new AtomicInteger(0);
	/**
	 * 处理中文件的缓存内容
	 */
	private List<FileLine> subFileCache;
	/**
	 * 处理中文件的缓存内容的字节数
	 */
	private int subFileCacheSize;
	
	private boolean isDone = false;
	
	/**
	 * 从queue中读取文件行内容FileLine，写入子文件中，确保子文件大小<10M
	 */
	@Override
	public void run() {
		FileLine fileLine = null;
//		System.err.println(this.getTaskName()+" has begin.");
		//加try-catch，否则运行时异常无法catch住，导致task线程挂掉，但是task对应的queue还被不断写入数据。
		//TODO 当FileWriteTask线程终止后，queue可能还会被EventHandler写入数据，
		try {
			//这里会多做几次尝试，直到master置位isDone=true之后，再行退出
			while(!isDone) {
				fileLine = queue.poll(50, TimeUnit.MILLISECONDS);					
				if(fileLine == null) {
					continue;
				} 
				
				int totalSize = this.subFileCacheSize + fileLine.getLineSize();
				//缓存住的子文件内容已经大于上限
				if(totalSize >= this.fileSpiltter.getSubFileSizeLimit()) {
					writeSubFile();
				} 
				this.subFileCache.add(fileLine);
				this.subFileCacheSize += fileLine.getLineSize();
			}	
			
			//退出时，将task中剩余的文件缓存写入到磁盘
			writeSubFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		System.err.println(this.getTaskName()+" has finished.");
		
	}
	

	
	/**集中一次写入整个子文件 */
	private void writeSubFile() {
		int subFileNo = subFileCounter.getAndIncrement();
		String subFileName = fileSpiltter.genSubFileFullName(subFileNo);
		RandomAccessFile randomAccessFile = null;
		try { 
			randomAccessFile = new RandomAccessFile(subFileName,"rw");
			FileChannel writer = randomAccessFile.getChannel();
			ByteBuffer writerBuffer = writer.map(FileChannel.MapMode.READ_WRITE, 0, this.subFileCacheSize);
			for(FileLine fileLine : this.subFileCache) {
				try {
					writerBuffer.put(fileLine.getLineContent());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
			
			writerBuffer.clear();
			writerBuffer = null;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(randomAccessFile != null) {
				try {
					randomAccessFile.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		System.out.println(Thread.currentThread().getName()+" write one sub file to disk, fileName is: "+subFileName);
		this.writenSize += this.subFileCacheSize;
		
		//reset
		this.subFileCache.clear();
		//下面这行代码会导致线程阻塞 TODO
//		this.subFileCache = null;
		this.subFileCacheSize = 0;
	}
	
	public FileWriteTask(int taskSeq,  FileSpiltter fileSpiltter, BlockingQueue<FileLine> queue) {
		this.taskSeq = taskSeq;
		this.fileSpiltter = fileSpiltter;
		this.queue = queue;
		this.writenSize = 0;
		this.taskName = "FileWriteTask_"+this.taskSeq;
		this.subFileCache = new ArrayList<FileLine>();
	}
	
	
	public int getTaskSeq() {
		return taskSeq;
	}

	public void setTaskSeq(int taskSeq) {
		this.taskSeq = taskSeq;
	}

	public String getTaskName() {
		return taskName;
	}
	
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public int getWritenSize() {
		return writenSize;
	}

	public void setWritenSize(int writenSize) {
		this.writenSize = writenSize;
	}

	public boolean isDone() {
		return isDone;
	}

	public void setDone(boolean isDone) {
		this.isDone = isDone;
	}	

	public BlockingQueue<FileLine> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<FileLine> queue) {
		this.queue = queue;
	}

	@Override
	public String toString() {
		return "FileWriteTask [taskSeq=" + taskSeq + ", taskName=" + taskName + ", writenSize=" + writenSize
				 + ", subFileCacheSize=" + subFileCacheSize + "]";
	}

	
}
