package com.daoqidlv.filespilt.disruptor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.daoqidlv.filespilt.Constants;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * 文件读取任务
 * @author Administrator
 *
 */
public class FileReadTask extends Thread{
	/**
	 * 任务序号
	 */
	private int taskSeq;
	/**
	 * 任务名称
	 */
	private String taskName;
	/**
	 * 读取开始的点
	 */
	private int beginFilePointer;
	/**
	 * 读取结束的点
	 */
	private int endFilePointer;
	/**
	 * 待读取的总大小
	 */	
	private int toReadSize;
	/**
	 * 源文件全路径
	 */
	private String orignFileFullName;
	/**
	 * 实际读取的数据大小
	 */
	private int readedSize;
	/**
	 * 一次读取的字节数
	 */
	private int readSizeOneTime;
	
	private Disruptor<FileLine> disruptor;

	public FileReadTask(int taskSeq, int beginFilePointer, int endFilePointer, String orignFileFullName, Disruptor<FileLine> disruptor) {
		this.taskSeq = taskSeq;
		this.beginFilePointer = beginFilePointer;
		this.endFilePointer = endFilePointer;
		this.orignFileFullName = orignFileFullName;
		this.disruptor = disruptor;
		this.readSizeOneTime = 10*1024*1024;
		this.taskName = "FileReadTask_"+this.taskSeq;
		this.readedSize = 0;
		this.toReadSize = this.endFilePointer - this.beginFilePointer + 1;
	}

	
	/**
	 * 从指定文件目录读取文件内容，并将每行内容转换为FileLine对象，写入queue中。
	 * 行结束标志：当读取到/n 或者 /r字节时
	 */
	@Override
	public void run() {
		try {
			@SuppressWarnings("resource")
			FileInputStream orginFile = new FileInputStream(this.orignFileFullName);
			int index = this.beginFilePointer;
			int totalSpiltedSize = 0;
			MappedByteBuffer inputBuffer = null;
			//如果是整行，则改行最后两个字符是/r/n，所以index小于this.endFilePointer才需要进入
			for(; index < this.endFilePointer;) {
				totalSpiltedSize = Math.min(this.readSizeOneTime, this.endFilePointer-index+1);
				inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
								index, totalSpiltedSize);
				try {
					index = spiltFileLine(inputBuffer, index, totalSpiltedSize);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
				
			}
			
			//对于最后一个task，可能最后一行并不含/r/n符号，还存在剩余未拆分的文件行内容，直接当着单独的行写入
			if(this.endFilePointer - index >= 0) {
				int fileLineSize = this.endFilePointer - index ;
				byte[] fileLineCache = new byte[fileLineSize];
				inputBuffer.get(fileLineCache, 0, fileLineSize);
				//将消息放入Disruptor中
				this.produceEventToDisruptor(fileLineSize, fileLineCache);
//					System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
				this.readedSize += fileLineSize;
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void produceEventToDisruptor(int lineSize, byte[] lineContent) {
		RingBuffer<FileLine> ringBuffer = this.disruptor.getRingBuffer();
		long sequence = ringBuffer.next();  // Grab the next sequence
        try
        {
            FileLine event = ringBuffer.get(sequence); // Get the entry in the Disruptor
                                                        // for the sequence
            event.setLineContent(lineContent);  // Fill with data
            event.setLineSize(lineSize);
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
	}
	
	
	/**
	 * 从字节缓存中拆分出每一行对应的字节数据，并放入队列
	 * @param inputBuffer 目标字节缓存
	 * @param index 进行拆分的起始字节位置
	 * @param totalSpiltedSize 总共需要进行拆分的字节数
	 * @return index 拆分处理后，最新的待处理字节的起始位置
	 * @throws Exception 
	 * @TODO 可以直接使用RandomAccessFile.readLine()方法
	 */
	int spiltFileLine(MappedByteBuffer inputBuffer, int index, int totalSpiltedSize) throws Exception {
		int subIndex = index;
		boolean newLineFlag = false;
		for(int i = 0; i < totalSpiltedSize-1; i++) {
			//遇到回车/换行符
			if(Constants.ENTER_CHAR_ASCII == inputBuffer.get(i) && Constants.NEW_LINE_CHAR_ASCII == inputBuffer.get(i+1)) {
				newLineFlag = true;
				//将/r后面的/n字节也包含到行中
				i++;
				int fileLineSize = subIndex+i+1-index;
				byte[] fileLineCache = new byte[fileLineSize];
				inputBuffer.get(fileLineCache, 0, fileLineSize);
//				FileLine fileLine = new FileLine(fileLineSize, fileLineCache);
				
				//将消息放入Disruptor中
				this.produceEventToDisruptor(fileLineSize, fileLineCache);
//					System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
				this.readedSize += fileLineSize;
				index += fileLineSize;
			}
		}
		if(!newLineFlag) {
			//最后一行，单独写入
			if(totalSpiltedSize < this.readSizeOneTime) {
				byte[] tempBytes = new byte[totalSpiltedSize];
				inputBuffer.get(tempBytes, 0, totalSpiltedSize);
//				FileLine fileLine = new FileLine(totalSpiltedSize, tempBytes);
				//将消息放入Disruptor中
				this.produceEventToDisruptor(totalSpiltedSize, tempBytes);
//					System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
				this.readedSize += totalSpiltedSize;
				index += totalSpiltedSize;
				//文件行内容过大，无法按行读取，需要调节readSizeOneTime参数
			} else {
				System.err.println("index="+index+",totalSpiltedSize="+totalSpiltedSize);
				
				System.err.println("this content has no new line, and its length has greater than "+this.readSizeOneTime);	
				throw new Exception("this content has no new line, and its length has greater than "+this.readSizeOneTime) ;				
			}
		}
		return index;
	}
	
	private void writeSubFile(MappedByteBuffer inputBuffer, int index, int totalSpiltedSize) {
		String subFileName = "D:\\temp\\error.txt";
		
		byte[] tempBytes = new byte[totalSpiltedSize];
		inputBuffer.get(tempBytes, 0, totalSpiltedSize);
		
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(subFileName);
			fos.write(tempBytes);
			fos.flush();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public int getTaskSeq() {
		return taskSeq;
	}

	public void setTaskSeq(int taskSeq) {
		this.taskSeq = taskSeq;
	}

	public int getBeginFilePointer() {
		return beginFilePointer;
	}

	public void setBeginFilePointer(int beginFilePointer) {
		this.beginFilePointer = beginFilePointer;
	}

	public int getToReadSize() {
		return toReadSize;
	}

	public void setToReadSize(int toReadSize) {
		this.toReadSize = toReadSize;
	}

	public String getOrignFileFullName() {
		return orignFileFullName;
	}

	public void setOrignFileFullName(String orignFileFullName) {
		this.orignFileFullName = orignFileFullName;
	}
	
	
	public int getReadedSize() {
		return readedSize;
	}

	public void setReadedSize(int readedSize) {
		this.readedSize = readedSize;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public int getEndFilePointer() {
		return endFilePointer;
	}

	public void setEndFilePointer(int endFilePointer) {
		this.endFilePointer = endFilePointer;
	}

	public int getReadSizeOneTime() {
		return readSizeOneTime;
	}

	public void setReadSizeOneTime(int readSizeOneTime) {
		this.readSizeOneTime = readSizeOneTime;
	}


	@Override
	public String toString() {
		return "FileReadTask [taskSeq=" + taskSeq + ", taskName=" + taskName + ", beginFilePointer=" + beginFilePointer
				+ ", endFilePointer=" + endFilePointer + ", toReadSize=" + toReadSize + ", orignFileFullName="
				+ orignFileFullName + ", readedSize=" + readedSize + ", readSizeOneTime=" + readSizeOneTime + "]";
	}


}
