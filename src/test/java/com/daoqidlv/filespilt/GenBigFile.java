package com.daoqidlv.filespilt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 生成大文件
 * @author Administrator
 *
 */
public class GenBigFile {
	
	public static void main(String[] args) throws FileNotFoundException {
		String fileName = "D:\\temp\\test.csv";
		for(int i=0; i<1; i++) {
			WriteFileTask task = new WriteFileTask(fileName, i*1024*1024*1024, 1024*1024*1024);
			task.start();
		}
		
	}
	
	static class WriteFileTask extends Thread {
		
		private String fileName;
		
		private int offset;
		
		private int size;
		
		public WriteFileTask(String fileName, int offset, int size) {
			this.fileName = fileName;
			this.offset = offset;
			this.size = size;
		}
		
		@Override
		public void run() {
			try {
				RandomAccessFile file = new RandomAccessFile(fileName, "rw");
				file.seek(this.offset);	
				int length = 100;
				for(int i=0, j=128*10; i< size; i++, j--) {
					file.write(String.valueOf(i%10).getBytes());
					if(j % 128 == 0) {
						file.writeByte(13);
						file.writeByte(10);
						i += 2;
						

//						Random random = new Random();
//						length = 100*random.nextInt(10);
//						if(length == 0) {
//							length = 100;
//						}
						if(j == 128) {
							j = 128*10;
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
	}

}
