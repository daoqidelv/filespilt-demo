package com.daoqidlv.filespilt.mutil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.daoqidlv.filespilt.Constants;
import com.daoqidlv.filespilt.FileSpiltter;

public class TaskAllocater {
	
	/**
	 * 原始文件全称
	 */
	private String orignFileFullName;
	
	/**
	 * 读操作任务数
	 */
	private int readTaskNum;
	
	/**
	 * 写操作任务数	
	 */
	private int writeTaskNum;
	/**
	 * 行最大字节数
	 */
	private int maxLineSize;
	/**
	 * 文件切割器
	 */
	private FileSpiltter fileSpiltter;
	
	/**
	 * 用于交换子文件内容的阻塞队列
	 */
	private BlockingQueue<FileLine> queue;
	
	public TaskAllocater(String orignFileFullName, int readTaskNum, int writeTaskNum, int maxLineSize, FileSpiltter fileSpiltter, BlockingQueue<FileLine> queue) {
		this.orignFileFullName = orignFileFullName;
		this.readTaskNum = readTaskNum;
		this.writeTaskNum = writeTaskNum;
		this.maxLineSize = maxLineSize;
		this.fileSpiltter = fileSpiltter;
		this.queue = queue;
	}
	
	/**
	 * 依据原始文件大小及读文件任务数，完成FileReadTask的初始化
	 * @return List<FileReadTask> FileReadTask任务列表
	 */
	public List<FileReadTask> initFileReadTask() {
		if(this.readTaskNum <= 0) {
			throw new IllegalArgumentException("文件读取任务数量必须大于0！");
		}
		
		 RandomAccessFile orginFile = null;
		 int orignFileSize = 0;
		 try {
			orginFile = new RandomAccessFile(this.orignFileFullName, "r");
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
				task = new FileReadTask(i, lastEndFilePointer+1, orignFileSize-1, this.orignFileFullName, this.queue);
			} else {
				revisedEndFilePointer = reviseEndFilePointer(lastEndFilePointer+avgToReadSize, orginFile);
				task = new FileReadTask(i, lastEndFilePointer+1, revisedEndFilePointer, this.orignFileFullName, this.queue);
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
		byte[] tempBytes = new byte[this.maxLineSize];
		try {
			orginFile.seek(endFilePointer-this.maxLineSize+1);
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

	/**
	 * 依据指定的readTaskNum初始化文件写入任务列表。
	 * @return List<FileWriteTask> —— 文件写入任务列表
	 */
	public List<FileWriteTask> initFileWriteTask() {
		 if(this.writeTaskNum <= 0) {
			throw new IllegalArgumentException("文件写入任务数量必须大于0！");
		}

		List<FileWriteTask> taskList = new ArrayList<FileWriteTask>();
		for(int i=0; i<this.writeTaskNum; i++) {
			FileWriteTask task = new FileWriteTask(i, this.fileSpiltter, this.queue);			
			taskList.add(task);
			System.out.println("创建一个FileWriteTask："+task);
		}
		return taskList;
	}
	
	public String getOrignFileFullName() {
		return orignFileFullName;
	}

	public void setOrignFileFullName(String orignFileFullName) {
		this.orignFileFullName = orignFileFullName;
	}

	public int getReadTaskNum() {
		return readTaskNum;
	}

	public void setReadTaskNum(int readTaskNum) {
		this.readTaskNum = readTaskNum;
	}

	public int getWriteTaskNum() {
		return writeTaskNum;
	}

	public void setWriteTaskNum(int writeTaskNum) {
		this.writeTaskNum = writeTaskNum;
	}

	public int getMaxLineSize() {
		return maxLineSize;
	}

	public void setMaxLineSize(int maxLineSize) {
		this.maxLineSize = maxLineSize;
	}

}
