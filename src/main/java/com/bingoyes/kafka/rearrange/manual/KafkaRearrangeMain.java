package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Configuration
@PropertySource(value = {"file:/opt/conf/kafka-stream.yml"})
@ConfigurationProperties(prefix = "kafkasource")
public class KafkaRearrangeMain {

    private static Logger logger = LoggerFactory.getLogger(KafkaRearrangeMain.class);

    private final static String portal_file = "/opt/conf/portal.yml";

    private List<TopicInputProcessor> threadList = new ArrayList<>();

    //允许最慢topic和最快topic之间的最相差窗口数
    private long allowMaxAHeadWindowNum=2;
    //最慢的topic当前窗口标识(窗口开始时间）
    private long slowestThreadLatestWindowId =0;
    //最慢topci线程Id
    private long slowestThreadIndex = -1;

    private int windowSize=120; //单位秒

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
            topicConfig.put("topic2",topicConfig.get("topic").toString()+ "_rearranged0223B");
            KafkaSinkService kafkaSinkService = new KafkaSinkService(topicConfig);

            TopicInputProcessor thread = new TopicInputProcessor(index++, this, kafkaSourceService, kafkaSinkService);
            threadList.add(thread);
            thread.start();
        }

        //new InputProcessorResumeThread().start();

    }

    public void workOnSlowestThread(){
        TopicInputProcessor slowestThread = this.getSlowestThread();
        this.setSlowestThreadIndex(slowestThread.getThreadIndex());
        this.setSlowestThreadLatestWindowId(slowestThread.getLatestProcessedWindowId());

    }

    public TopicInputProcessor getSlowestThread(){
        TopicInputProcessor[] threadArray = threadList.toArray(new TopicInputProcessor[]{});

        for(int j=0;j<threadList.size()-1;j++){
           if(threadArray[j].getLatestProcessedWindowId()<threadArray[j+1].getLatestProcessedWindowId()) {
               TopicInputProcessor temp = threadArray[j + 1];
               threadArray[j + 1] = threadArray[j];
               threadArray[j] = temp;
           }
       }
       return threadArray[threadArray.length-1];
    }

    //重启数据输入进程（由于该进程的处理的window过去超前，数据输入进程被暂停
    class InputProcessorResumeThread extends Thread{

        public void run(){
            while(true){
                System.out.println("InputProcessorResumeThread:");

                for(TopicInputProcessor processor:threadList){
                    //如果窗口数差值在允许范围内，则重新开始输入输入
                    if(processor.getLatestProcessedWindowId()-getSlowestThreadLatestWindowId()/windowSize<allowMaxAHeadWindowNum){
                        if(processor.getInputLatch().getCount()>0) {
                            processor.getInputLatch().countDown();
                            System.out.println("resume processor:" +processor.getThreadIndex());

                        }

                    }
                }

                try {
                    sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
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

    public int getWindowSize() {
        return windowSize;
    }

    public static void  main(String[] args){
        new KafkaRearrangeMain().startAllThread();
    }
}


