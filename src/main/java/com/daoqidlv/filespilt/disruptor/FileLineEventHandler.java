package com.daoqidlv.filespilt.disruptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.lmax.disruptor.EventHandler;

public class FileLineEventHandler implements EventHandler<FileLine>{
	
	private List<BlockingQueue<FileLine>> consumerQueues;

	/**
	 * 当前轮岗的消费者queue序号
	 */
	private int queueSeq = 0;
	
	public  FileLineEventHandler(List<BlockingQueue<FileLine>> consumerQueues) {
		this.consumerQueues = consumerQueues;
	}
	
	public FileLineEventHandler() {
		this.consumerQueues = new ArrayList<BlockingQueue<FileLine>>();
	}
	
	

	public List<BlockingQueue<FileLine>> getConsumerQueues() {
		return consumerQueues;
	}

	public void setConsumerQueues(List<BlockingQueue<FileLine>> consumerQueues) {
		this.consumerQueues = consumerQueues;
	}
	
	public void addConsumerQueue(BlockingQueue<FileLine> consumerQueue) {
		this.consumerQueues.add(consumerQueue);
	}


	public void onEvent(FileLine event, long sequence, boolean endOfBatch) throws Exception {
		if(this.consumerQueues == null && this.consumerQueues.size() <= 0) {
			System.err.println("concumer queues size is ZERO, return.");
			return;
		}		

		//NOTE: 不能直接使用Disruptor的event对象，因为RingBUffer中的这些对象会被重复使用。所以这里对event进行深度克隆产生新的对象写入到FileWriterTask私有的queue中。
		FileLine fileLine = event.clone();		

		boolean hasProceed = false;
		
		/**
		 * 方式一： 不断轮训消费者的私有queue，如果能放入则直接放入，否则寻找下一个queue，如果所有的queue均无空闲，则自旋5s后继续
		 * 缺点：小序号的queue对应的消费者会非常忙碌，其他消费者得不到利用。
		 */
//		do {
//			
//			for(Iterator<BlockingQueue<FileLine>> it = consumerQueues.iterator(); it.hasNext();) {
//				BlockingQueue<FileLine> queue = it.next();
//				
//				hasProceed = queue.offer(fileLine);
//				if(hasProceed) {
//					break;
//				}
//			}
//			//TODO
//			if(!hasProceed) {
//				Thread.sleep(5);
//			}
//		} while(!hasProceed);
		
		
		/**
		 * 方式二：每次按照顺序循环箱消费者的queues放入消息，如果当前queue已满，则寻找下一个可用的queue
		 * 优点：消费者之间平等消费
		 * 缺点：如果某个消费者的消费能力很弱，则会造成任务堆积过多，从而影响整个调度
		 * 适用场景：适合于任务耗时相当的任务
		 */

		int offset = consumerQueues.size() - 1;
		do {
			queueSeq = queueSeq & offset;

			BlockingQueue<FileLine> queue = consumerQueues.get(queueSeq);
			hasProceed = queue.offer(fileLine);
			
			queueSeq++;
			
			if(hasProceed) {
				break;
			}
			
		} while(!hasProceed);
	}
	

}
