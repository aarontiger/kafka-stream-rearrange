package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.stream.rearrange.ThreadStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class RearrangeMain {

    private List<RearrangeThread> threadList = new ArrayList<>();

    private List<String> topicList;

    long topicWatermark;
    private long topicMaxOutOfOrderness;

    public void prepareKafkaConfig(){

    }

   public void startAllThread(){

        for(String topic:topicList) {
            KafkaSourceService kafkaSourceService = new KafkaSourceService(topic);
            KafkaSinkService kafkaSinkService = new KafkaSinkService(topic+"_rearranged");
            RearrangeThread thread =new RearrangeThread(kafkaSourceService,kafkaSinkService);
            threadList.add(thread);
            thread.start();
        }

   }

}


