package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaRearrangeMain {

    private static Logger logger = LoggerFactory.getLogger(KafkaRearrangeMain.class);

    private final static String portal_file = "/opt/conf/portal.yml";

    private List<OneTopicRearrangeProcessor> threadList = new ArrayList<>();

    //允许最慢topic和最慢topic之间的最大差值
    private long allowMaxAHeadWindowNum=10;
    //最慢的topic当前窗口标识(窗口开始时间）
    private long slowestThreadLatestWindowId =0;
    //最慢topci线程Id
    private long slowestThreadIndex = -1;

    public void startAllThread() {

        KafkaConfigUtil kafkaConfigUtil = new KafkaConfigUtil();

        List<Map> topicConfigList = kafkaConfigUtil.getTopicConfigList();

        int index = 0; //线程索引编号，从0开始
        for (Map topicConfig : topicConfigList) {
            topicConfig.put("uri",kafkaConfigUtil.getUri());
            topicConfig.put("auth",kafkaConfigUtil.isAuth());
            topicConfig.put("user",kafkaConfigUtil.getUser());
            topicConfig.put("password",kafkaConfigUtil.getPassword());

            KafkaSourceService kafkaSourceService = new KafkaSourceService(topicConfig);
            topicConfig.put("topic2",topicConfig.get("topic").toString()+ "_rearranged");
            KafkaSinkService kafkaSinkService = new KafkaSinkService(topicConfig);

            OneTopicRearrangeProcessor thread = new OneTopicRearrangeProcessor(index++, this, kafkaSourceService, kafkaSinkService);
            threadList.add(thread);
            thread.start();
        }

    }

    public OneTopicRearrangeProcessor getSlowestThread(){
        OneTopicRearrangeProcessor[] threadArray = threadList.toArray(new OneTopicRearrangeProcessor[]{});

        for(int j=0;j<threadList.size()-1;j++){
           if(threadArray[j].getLatestProcessedWindowId()<threadArray[j+1].getLatestProcessedWindowId()) {
               OneTopicRearrangeProcessor temp = threadArray[j + 1];
               threadArray[j + 1] = threadArray[j];
               threadArray[j] = temp;
           }
       }
       return threadArray[threadArray.length-1];
    }

    public long getSlowestThreadLatestWindowId() {
        return slowestThreadLatestWindowId;
    }

    public void setSlowestThreadLatestWindowId(long slowestThreadLatestWindowId) {
        this.slowestThreadLatestWindowId = slowestThreadLatestWindowId;
    }

    public long getSlowestThreadIndex() {
        return slowestThreadIndex;
    }

    public void setSlowestThreadIndex(long slowestThreadIndex) {
        this.slowestThreadIndex = slowestThreadIndex;
    }

    public long getAllowMaxAHeadWindowNum() {
        return allowMaxAHeadWindowNum;
    }

    public void setAllowMaxAHeadWindowNum(long allowMaxAHeadWindowNum) {
        this.allowMaxAHeadWindowNum = allowMaxAHeadWindowNum;
    }

    public static void  main(String[] args){
        new KafkaRearrangeMain().startAllThread();
    }
}


