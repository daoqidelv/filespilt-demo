package com.daoqidlv.filespilt.mutil;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;

import com.daoqidlv.filespilt.mutil.FileLine;
import com.daoqidlv.filespilt.mutil.FileReadTask;

public class FileReadTaskTest {

	private FileInputStream  orginFile;
	
	private String orignFileFullName;
	
	private BlockingQueue<FileLine> queue;
	
	
	
	@Before 
	public void init() throws FileNotFoundException {
		orignFileFullName = "/Users/dujiacheng.jason/IdeaProjects/filespilt-demo/src/test/java/com/daoqidlv/filespilt/mutil/test.csv";
		orginFile = new FileInputStream (orignFileFullName);
		
		//int taskSeq, int beginFilePointer, int endFilePointer, String orignFileFullName, int maxLineSize
		queue = new LinkedBlockingQueue<FileLine>(100);
		
	} 
	
	@Test
	public void shouldSpiltFileLineTaskSuccess() throws Exception {
		//第一个任务的检查
		FileReadTask fileReadTask = new FileReadTask(0, 0, 23, orignFileFullName, queue);
		MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
				fileReadTask.getBeginFilePointer(), fileReadTask.getToReadSize());
		int index = fileReadTask.spiltFileLine(inputBuffer, fileReadTask.getBeginFilePointer(), fileReadTask.getToReadSize());

		assertTrue(this.queue.size() == 2);
		assertTrue(index == 24);
		this.queue.clear();
		inputBuffer = null;
		
//		//最后一个任务的检查
//		FileReadTask fileReadTask2 = new FileReadTask(3, 70, 97, orignFileFullName, queue);
//		FileChannel fileChannel = orginFile.getChannel();
////		fileChannel.map(FileChannel.MapMode.READ_ONLY,0, 70);
//		MappedByteBuffer inputBuffer2 = fileChannel.map(FileChannel.MapMode.READ_ONLY, 70, 28);
//		System.err.println("current position is "+inputBuffer2.position());
//		int index2 = fileReadTask2.spiltFileLine(inputBuffer2, fileReadTask2.getBeginFilePointer(), fileReadTask2.getToReadSize());
//		assertTrue(this.queue.size() == 4);
//		assertTrue(index2 == 97);
	}
}
