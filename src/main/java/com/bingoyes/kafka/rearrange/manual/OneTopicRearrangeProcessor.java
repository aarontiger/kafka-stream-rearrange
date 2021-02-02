package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.QuickSortUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class OneTopicRearrangeProcessor extends Thread {
    private static Logger logger = LoggerFactory.getLogger(OneTopicRearrangeProcessor.class);

    private int threadIndex;

    private KafkaRearrangeMain context;


    private KafkaService kafkaService;

    private long latestEventTime = 0;
    //private CountDownLatch latch= new CountDownLatch(0);

    public OneTopicRearrangeProcessor(int threadIndex, KafkaRearrangeMain context, KafkaService kafkaService){
        this.threadIndex = threadIndex;
        this.context = context;
        this.kafkaService = kafkaService;
    }

    @Override
    public void run(){


        while(true){

       /*     try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/


            List<MessageRecord> recordList = kafkaService.readMessage(600,4);
            System.out.println("thread:"+threadIndex+",get record size:"+recordList.size());

            if(recordList.size()>0) {

                MessageRecord[] listArray = recordList.toArray(new MessageRecord[]{});

                QuickSortUtil.quickSort(listArray);
                logger.info("thread:"+threadIndex+",sort success");
                for(MessageRecord messageRecord:listArray){
                    logger.info("timestamp:"+messageRecord.getTimestamp());
                }
                kafkaService.sendMessage(listArray);
                logger.info("thread:"+threadIndex+",send message success");

                /*latestEventTime = listArray[listArray.length].getTimestamp();
                context.caculateLowesThread();
                if (latestEventTime - context.getSlowestThreadEventTime() > context.getAllowMaxAHeadSeconds())
                    latch = new CountDownLatch(1);*/

                //commit
                Map<TopicPartition, OffsetAndMetadata>  maxOffset = recordList.get(recordList.size()-1).currentOffsets;
                kafkaService.commit(maxOffset);
            }
        }
    }

    public int getThreadIndex() {
        return threadIndex;
    }

    public long getLatestEventTime() {
        return latestEventTime;
    }
}
