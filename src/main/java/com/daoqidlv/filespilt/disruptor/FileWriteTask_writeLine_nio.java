package com.daoqidlv.filespilt.disruptor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.daoqidlv.filespilt.FileSpiltter;

/**
 * 文件写入任务。
 * 将文件内容写入到子文件中，写入时完成子文件自动生成和切换。
 * 
 * 子文件逐行写入，使用NIO
 * 
 * TODO 存在功能上的缺陷：由于预先分配好了ByteBuffer的大小，当文件内容不足时，会存在很多空NUL的字节，使得文件内容失真。
 * 
 * @author Administrator
 *
 */
public class FileWriteTask_writeLine_nio extends Thread {
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
	 * 处理中文件的缓存内容的字节数
	 */
	private int subFileCacheSize;
	
	
	private RandomAccessFile randomAccessFile;
	
	private ByteBuffer writerBuffer;
	
	private volatile static boolean isDone = false;
	
	/**
	 * 从queue中读取文件行内容FileLine，写入子文件中，确保子文件大小<10M
	 */
	@Override
	public void run() {
		FileLine fileLine = null;
		//打开文件流
		openWriteByteBuffer();
		
		//这里会多做几次尝试，直到master置位isDone=true之后，再行退出
		try {
			while(!isDone) {
				fileLine = queue.poll(50, TimeUnit.MILLISECONDS);	
				
				if(fileLine == null) {
					continue;
				}	
				int totalSize = this.subFileCacheSize + fileLine.getLineSize();
				//缓存住的子文件内容已经大于上限
				if(totalSize >= this.fileSpiltter.getSubFileSizeLimit()) {
					//当前文件已写满，切换文件，关闭现有文件流，并将文件对象置为null
					switchSubFile();
				} 
				//将当前行写入
				writeSubFile(fileLine);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		closeWriteByteBuffer();
	}
	

	

	

	
	private void openWriteByteBuffer() {
		int subFileNo = subFileCounter.getAndIncrement();
		String subFileName = fileSpiltter.genSubFileFullName(subFileNo);
		try {
			randomAccessFile = new RandomAccessFile(subFileName,"rw");
			FileChannel writer = randomAccessFile.getChannel();
			writerBuffer = writer.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSpiltter.getSubFileSizeLimit());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(Thread.currentThread().getName()+" new one sub file to disk, fileName is: "+subFileName);
	}
	
	private void closeWriteByteBuffer() {
		if(randomAccessFile != null) {
			try {
				randomAccessFile.close();
				randomAccessFile = null;
				writerBuffer = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void switchSubFile() {
		closeWriteByteBuffer();
		openWriteByteBuffer();
		this.subFileCacheSize = 0;
	}
	
	private void writeSubFile(FileLine fileLine) {
		if(this.writerBuffer == null) {
			openWriteByteBuffer();
		}
		
		this.writerBuffer.put(fileLine.getLineContent());

		this.subFileCacheSize += fileLine.getLineSize();	
		this.writenSize += fileLine.getLineSize();
	}
	
	public FileWriteTask_writeLine_nio(int taskSeq,  FileSpiltter fileSpiltter, BlockingQueue<FileLine> queue) {
		this.taskSeq = taskSeq;
		this.fileSpiltter = fileSpiltter;
		this.queue = queue;
		this.writenSize = 0;
		this.taskName = "FileWriteTask_"+this.taskSeq;
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

	public static boolean isDone() {
		return isDone;
	}
	
	

	public BlockingQueue<FileLine> getQueue() {
		return queue;
	}







	public void setQueue(BlockingQueue<FileLine> queue) {
		this.queue = queue;
	}







	public static void setDone(boolean isDone) {
		FileWriteTask_writeLine_nio.isDone = isDone;
	}

	@Override
	public String toString() {
		return "FileWriteTask [taskSeq=" + taskSeq + ", taskName=" + taskName + ", writenSize=" + writenSize
				 + ", subFileCacheSize=" + subFileCacheSize + "]";
	}

	
}
