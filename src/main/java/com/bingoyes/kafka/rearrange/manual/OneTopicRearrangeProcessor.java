package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.QuickSortUtil;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class OneTopicRearrangeProcessor extends Thread {
    private int threadIndex;

    private KafkaRearrangeMain context;


    private KafkaSourceService kafkaSourceService;
    private KafkaSinkService kafkaSinkService;


    ///////

    private long latestEventTime = 0;
    private CountDownLatch latch= new CountDownLatch(0);

    public OneTopicRearrangeProcessor(int threadIndex, KafkaRearrangeMain context, KafkaSourceService kafkaSourceService, KafkaSinkService kafkaSinkService){
        this.threadIndex = threadIndex;
        this.context = context;
        this.kafkaSourceService = kafkaSourceService;
        this.kafkaSinkService = kafkaSinkService;
    }

    @Override
    public void run(){


        while(true){


            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            List<MessageRecord> recordList = kafkaSourceService.readMessage(300);
            System.out.println("thread:"+threadIndex+",get record size:"+recordList.size());

            List<MessageRecord> resultList = new ArrayList<>();
            MessageRecord[] listArray = recordList.toArray(new MessageRecord[]{});

            QuickSortUtil.quickSort(listArray);

            kafkaSinkService.sendMessage(listArray);

            latestEventTime = listArray[listArray.length].getTimestamp();
            if(latestEventTime-context.getSlowestThreadEventTime()>context.getAllowMaxAHeadSeconds())
                latch =new CountDownLatch(1);
        }
    }




    public KafkaSinkService getKafkaSinkService() {
        return kafkaSinkService;
    }

    public void setKafkaSinkService(KafkaSinkService kafkaSinkService) {
        this.kafkaSinkService = kafkaSinkService;
    }



    public int getThreadIndex() {
        return threadIndex;
    }

    public long getLatestEventTime() {
        return latestEventTime;
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
