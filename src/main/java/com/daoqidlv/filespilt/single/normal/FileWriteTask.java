package com.daoqidlv.filespilt.single.normal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.daoqidlv.filespilt.Util;

/**
 * 文件写入任务类，记录每个文件写入任务信息，且完成文件写入。
 * @author daoqidelv
 * @CreateDate 2017年5月4日
 *
 */
public class FileWriteTask extends Thread{
	
	/**
	 * 文件所在路径
	 */
	private String fileDir;
	/**
	 * 文件名称
	 */
	private String fileName;
	
	/**
	 * 文件内容
	 */
	private List<String> fileContent;
	
	/**
	 * 文件大小
	 */
	private int fileSize;
	
	/**
	 * 文件实际写入的大小
	 */
	private int fileWritenSize;
	
	public FileWriteTask(String fileDir, String fileName, List<String> fileContent, int fileSize) {
		this.fileName = fileName;
		this.fileContent = fileContent;
		this.fileDir = fileDir;
		this.fileSize = fileSize;
		this.fileWritenSize = 0;
	}
	
	
	@Override
	public void run() {
		File file = new File(Util.genFullFileName(this.fileDir, this.fileName));
		BufferedWriter bw = null;
		try {
			FileWriter fw = new FileWriter(file);
			bw = new BufferedWriter(fw);
			for(String lineContent : fileContent) {
				bw.write(lineContent);
				bw.newLine();
				bw.flush();
				fileWritenSize += lineContent.length();
			}
			System.out.println("写入一个子文件，文件名为："+this.fileName+", 文件大小为："+this.fileSize);
		} catch (FileNotFoundException e) {
			//TODO 日志记录
			System.err.println("写文件错误！");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("写文件错误！");
			e.printStackTrace();
		} finally {
			if(bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					System.err.println("关闭文件流错误！");
					e.printStackTrace();
				}
			}
			this.fileContent = null;
		}
	}


	public String getFileDir() {
		return fileDir;
	}


	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}


	public String getFileName() {
		return fileName;
	}


	public void setFileName(String fileName) {
		this.fileName = fileName;
	}


	public List<String> getFileContent() {
		return fileContent;
	}


	public void setFileContent(List<String> fileContent) {
		this.fileContent = fileContent;
	}


	public int getFileSize() {
		return fileSize;
	}


	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}


	public int getFileWritenSize() {
		return fileWritenSize;
	}


	public void setFileWritenSize(int fileWritenSize) {
		this.fileWritenSize = fileWritenSize;
	}
	
}
