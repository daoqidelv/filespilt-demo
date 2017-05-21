package com.daoqidlv.filespilt;

public class Constants {

	
	public final static String FILENAME_SEPARATOR = "\\.";
	
	public final static String MASTER_TYPE_NORMAL_POOL = "NORMAL";	
	
	public final static String MASTER_TYPE_FORK_JOIN_POOL = "FORKJOIN";
	
	public final static String MASTER_TYPE_FORK_PRODUCER_CONSUMER = "PRODUCERCONSUMER";	
	
	public final static String DEFAULT_MASTER_TYPE = MASTER_TYPE_FORK_JOIN_POOL;	
	
	/**
	 * 换行符的ASCII码
	 */
	public final static int NEW_LINE_CHAR_ASCII = 10;
	/**
	 * 回车符的ASCII码
	 */
	public final static int ENTER_CHAR_ASCII = 13;

}
