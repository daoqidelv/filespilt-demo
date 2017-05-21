package com.daoqidlv.filespilt.mutil;

import java.util.Arrays;

/**
 * 文件中的一行，作为读/写任务的最小执行单元。
 * @author Administrator
 *
 */
public class FileLine {
	
	/**
	 * 行字节大小，单位byte
	 */
	private int lineSize;
	/**
	 * 行内容字节数组
	 */
	private byte[] lineContent;
	
	public FileLine(int lineSize, byte[] lineContent) {
		this.lineSize = lineSize;
		this.lineContent = lineContent;
	}

	public int getLineSize() {
		return lineSize;
	}

	public void setLineSize(int lineSize) {
		this.lineSize = lineSize;
	}

	public byte[] getLineContent() {
		return lineContent;
	}

	public void setLineContent(byte[] lineContent) {
		this.lineContent = lineContent;
	}

	@Override
	public String toString() {
		return "FileLine [lineSize=" + lineSize + ", lineContent=" + Arrays.toString(lineContent) + "]";
	}
	

}
