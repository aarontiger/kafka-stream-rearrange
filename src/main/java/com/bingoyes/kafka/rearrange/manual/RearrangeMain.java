package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.stream.rearrange.SingleStreamRearrangeThread;
import com.bingoyes.kafka.stream.rearrange.ThreadStatus;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class RearrangeMain {

    private List<ThreadStatus> threadStatusList;

    CountDownLatch latch1 = new CountDownLatch(1);

    CountDownLatch latch2 = new CountDownLatch(1);

    private List<String> topicList;

    long topicWatermark;
    private long topicMaxOutOfOrderness;

    public void prepareKafkaConfig(){

    }

   public void startAllThread(){

        for(String topic:topicList) {
            KafkaService kafkaService = new KafkaService(topic);
            new RearrangeThread(kafkaService).start();
        }

   }

}


