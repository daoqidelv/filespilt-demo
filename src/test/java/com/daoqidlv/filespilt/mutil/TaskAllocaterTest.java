package com.daoqidlv.filespilt.mutil;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;

import com.daoqidlv.filespilt.FileSpiltter;
import com.daoqidlv.filespilt.mutil.FileLine;
import com.daoqidlv.filespilt.mutil.FileReadTask;
import com.daoqidlv.filespilt.mutil.TaskAllocater;


public class TaskAllocaterTest {
	
	private TaskAllocater taskAllocater;
	
	private String testDirRootPath;
	
	@Before 
	public void init() { 

		File f = new File(this.getClass().getResource("").getPath()); 
		testDirRootPath = f.getAbsolutePath();
		
		String orignFileFullName = testDirRootPath+"\\test.csv";
		int readTaskNum = 4;
		int writeTaskNum = 4;
		int maxLineSize = 16;
		int subFileSizeLimit = 32;
		
		FileSpiltter fileSpiltter = new FileSpiltter(subFileSizeLimit, testDirRootPath, "test.csv");
		BlockingQueue queue = new LinkedBlockingQueue<FileLine>(1024);
		taskAllocater = new TaskAllocater(orignFileFullName, readTaskNum, writeTaskNum, maxLineSize, fileSpiltter, queue);
	} 
	
	@Test
	public void shouldInitFileReadTaskSuccess() {
		List<FileReadTask> tasks = taskAllocater.initFileReadTask();
		assertTrue(tasks != null && tasks.size() == 4);
		for(FileReadTask task : tasks) {
			System.out.println(task);
			switch(task.getTaskSeq()) {
				case 0:assertTrue(task.getEndFilePointer() == 23 && task.getToReadSize() == 24);break;
				case 1:assertTrue(task.getEndFilePointer() == 47 && task.getToReadSize() == 24);break;
				case 2:assertTrue(task.getEndFilePointer() == 69 && task.getToReadSize() == 22);break;
				case 3:assertTrue(task.getEndFilePointer() == 97 && task.getToReadSize() == 28);break;
				default:assertTrue(false);
			}
		}
	}

}
