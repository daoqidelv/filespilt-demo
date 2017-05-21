java实现大文件拆分示例代码
=========
本示例程序实现了两种大文件拆分的方案
----------

*    单线程读多线程写方案，该方案使用了两种不同的线程池实现：ThreadPoolExcutor和ForkJoinPool，分别对应NORMAL和FORKJOIN两种执行模式；  
*    生产者-消费者模式下的多线程读/写方案，对应PRODUCERCONSUMER执行模式。  
	
程序目录结构
----------

*    com.daoqidlv.filespilt —— 公共类，及程序入口类	  
*    com.daoqidlv.filespilt.single.normal —— NORMAL模式的具体实现类	  
*    com.daoqidlv.filespilt.single.forkjoin —— FORKJOIN模式的具体实现类	  
*    com.daoqidlv.filespilt.mutil —— PRODUCERCONSUMER模式下的具体实现类  
	
示例程序介绍
----------

入口类：
    com.daoqidlv.filespilt.Test.java  

执行命令格式：
    java -jar fileapilt.jar #fileDir #fileName #subFileSizeLimit #mode [#readTaskNum #writeTaskNum #queueSize]  
1.    #root_dir —— 源文件及拆分后子文件放置的根目录    		
2.    #orign_file_name —— 原文件名  
3.    #subFileSizeLimit —— 拆分后的子文件大小上限值，开区间    
4.    #mode —— 执行模式：NORMAL -- 使用普通线程池，FORKJOIN -- 使用ForkJoinPool， PRODUCERCONSUMER --生产者-消费者模式  
5.    #readTaskNum —— 可选。读任务数，当参数4为PRODUCERCONSUMER时有效，不输入则默认为24  
6.    #writeTaskNum —— 可选。写任务数，当参数4为PRODUCERCONSUMER时有效，不输入则默认为8  
7.    #queueSize —— 可选。任务队列大小，当参数4为PRODUCERCONSUMER时有效，不输入则默认为10240  

执行命令示例：  
*    NORMAL/FORKJOIN模式  
	java -jar fileapilt.jar D:\Users\daoqidelv\Desktop\alibaba localhost_access_log.txt 10 FORKJOIN  
*    PRODUCERCONSUMER模式   
	java -jar fileapilt.jar D:\Users\daoqidelv\Desktop\alibaba localhost_access_log.txt 10 PRODUCERCONSUMER 24 8 10240  
			
设计文档及相关讨论
------
[大文件拆分问题的java实践](http://www.cnblogs.com/daoqidelv/p/6884223.html)
		
		