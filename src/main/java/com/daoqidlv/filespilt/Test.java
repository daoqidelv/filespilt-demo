package com.daoqidlv.filespilt;


public class Test {

	/**
	 * 参数1：源文件所在目录
	 * 参数2：原文件名
	 * 参数3：子文件大小上限值，必须为整数，单位:M	 * 
	 * 参数4：执行模式：NORMAL -- 使用普通线程池，FORKJOIN -- 使用ForkJoinPool， PRODUCERCONSUMER --生产者-消费者模式
	 * 参数5：读任务数，当参数4为PRODUCERCONSUMER时有效，默认为24
	 * 参数6：写任务数，当参数4为PRODUCERCONSUMER时有效，默认为8
	 * 参数7：任务队列大小，当参数4为PRODUCERCONSUMER时有效，默认为10240
	 * 
	 * @param args 
	 * 
	 */
	public static void main(String[] args) {
		int readTaskNum = 24;
		int writeTaskNum = 8;
		int queueSize = 10240;
		//获取参数
		if(args != null) {
			if(args.length == 4) {
				
			} else if(args.length == 7) {
				readTaskNum = Integer.valueOf(args[4]);
				writeTaskNum = Integer.valueOf(args[5]);
				queueSize = Integer.valueOf(args[6]);
			} else {
				System.err.println("参数为空，示例：#fileDir, #fileName, #subFileSizeLimit, #mode [, #readTaskNum, #writeTaskNum, #queueSize]，");
				return;
			}				
		} else {
			System.err.println("参数为空，示例：#fileDir, #fileName, #subFileSizeLimit, #mode，");
			return;
		}
		String fileDir = args[0];
		String fileName = args[1];
		String subFileSizeLimitStr = args[2];
		String mode = args[3];


		//参数合法性校验
		if(Util.isNull(fileDir, fileName, subFileSizeLimitStr)) {
			System.err.println("部分参数为空！示例：#fileDir, #fileName, #subFileSizeLimit");
			return;
		}
		
		String[] fileNameItems = fileName.split(Constants.FILENAME_SEPARATOR);
		if(fileNameItems.length != 2) {
			System.err.println("参数fileName格式错误！示例：fileName.csv");
			return;
		}		
		
		if((mode != null && !mode.equals(Constants.MASTER_TYPE_FORK_JOIN_POOL) && (!mode.equals(Constants.MASTER_TYPE_NORMAL_POOL)) && (!mode.equals(Constants.MASTER_TYPE_FORK_PRODUCER_CONSUMER)))) {
			System.err.println("参数mode必须是'NORMAL' 或者 'FORKJOIN' 或者'PRODUCERCONSUMER'");
			return; 
		}

		int subFileSizeLimit = 0;
		try {
			subFileSizeLimit = Integer.valueOf(subFileSizeLimitStr);
		} catch (NumberFormatException e) {
			System.err.println("子文件大小上限值必须为整数。");
			return;
		}
		
		//通过工厂方法获取master实例
		Master master = Master.getMasterInstance(mode, fileDir, fileName, subFileSizeLimit, readTaskNum, writeTaskNum, queueSize);
		System.out.println("The master is: "+master.getClass().getName());
		master.init();
		//启动master
		master.excute();
		
	}
}
