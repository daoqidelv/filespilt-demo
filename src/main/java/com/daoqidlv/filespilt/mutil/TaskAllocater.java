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
    private String originFileFullName;

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

    public TaskAllocater(String originFileFullName, int readTaskNum, int writeTaskNum, int maxLineSize, FileSpiltter fileSpiltter, BlockingQueue<FileLine> queue) {
        this.originFileFullName = originFileFullName;
        this.readTaskNum = readTaskNum;
        this.writeTaskNum = writeTaskNum;
        this.maxLineSize = maxLineSize;
        this.fileSpiltter = fileSpiltter;
        this.queue = queue;
    }

    /**
     * 依据原始文件大小及读文件任务数，完成FileReadTask的初始化
     *
     * @return List<FileReadTask> FileReadTask任务列表
     */
    public List<FileReadTask> initFileReadTask() {
        if (this.readTaskNum <= 0) {
            throw new IllegalArgumentException("文件读取任务数量必须大于0！");
        }

        // 打开源文件
        RandomAccessFile originFile = null;
        int originFileSize = 0;
        try {
            originFile = new RandomAccessFile(this.originFileFullName, "r");  // 这个只是为了能够快速定位行尾指针，之后还会用别的文件形式真正读取
            originFileSize = (int) originFile.length();            //取得文件长度（字节数）
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int avgToReadSize = originFileSize / this.readTaskNum;        // 每个读线程要读的大小

        List<FileReadTask> taskList = new ArrayList<FileReadTask>();
        int lastEndFilePointer = -1;
        int revisedEndFilePointer = 0;


        for (int i = 0; i < this.readTaskNum; i++) {
            FileReadTask task = null;
            if (i == this.readTaskNum - 1) {
                task = new FileReadTask(i, lastEndFilePointer + 1, originFileSize - 1, this.originFileFullName, this.queue);  //最后一个任务将剩余未分配的数据也读取完成

            } else {
                revisedEndFilePointer = reviseEndFilePointer(lastEndFilePointer + avgToReadSize, originFile);     // 正常的会读avgToReadSize大小，用reviseEndFilePointer修正保证尾指针指到完整行
                task = new FileReadTask(i, lastEndFilePointer + 1, revisedEndFilePointer, this.originFileFullName, this.queue); // 创建read任务，传入首尾指针，可行破碎怎么办
                lastEndFilePointer = revisedEndFilePointer;
            }
            taskList.add(task); // 把任务加到读取任务列表
            System.out.println("创建一个FileReadTask：" + task);
        }
        return taskList;
    }

    /**
     * 修正任务的结束文件指针位置，加上第一个换行符/回车符 的偏移量，确保‘完整行’需求
     *
     * @param endFilePointer
     * @param originFile
     * @return revisedEndFilePointer —— 修正后的endFilePointer
     * TODO 此方案需要提前给定最大行大小，这个在实际情况时，很难有这个限定，待优化。
     */
    private int reviseEndFilePointer(int endFilePointer, RandomAccessFile originFile) {
        int revisedEndFilePointer = endFilePointer;
        byte[] tempBytes = new byte[this.maxLineSize];
        try {
            originFile.seek(endFilePointer - this.maxLineSize + 1);
            originFile.readFully(tempBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int revisedNum = 0;
        for (int i = tempBytes.length - 1; i >= 1; i--) {
            if (Constants.ENTER_CHAR_ASCII == tempBytes[i - 1] && Constants.NEW_LINE_CHAR_ASCII == tempBytes[i]) {             //换行符或者回车符，做修正
                break;
            } else {
                revisedNum++;
            }
        }
        return revisedEndFilePointer - revisedNum;
    }

    /**
     * 依据指定的readTaskNum初始化文件写入任务列表。
     *
     * @return List<FileWriteTask> —— 文件写入任务列表
     */
    public List<FileWriteTask> initFileWriteTask() {
        if (this.writeTaskNum <= 0) {
            throw new IllegalArgumentException("文件写入任务数量必须大于0！");
        }

        List<FileWriteTask> taskList = new ArrayList<FileWriteTask>();
        for (int i = 0; i < this.writeTaskNum; i++) {
            FileWriteTask task = new FileWriteTask(i, this.fileSpiltter, this.queue);
            taskList.add(task);
            System.out.println("创建一个FileWriteTask：" + task);
        }
        return taskList;
    }

    public String getoriginFileFullName() {
        return originFileFullName;
    }

    public void setoriginFileFullName(String originFileFullName) {
        this.originFileFullName = originFileFullName;
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
