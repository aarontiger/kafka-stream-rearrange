package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class KafkaRearrangeMain {

    private static Logger logger = LoggerFactory.getLogger(KafkaRearrangeMain.class);

    private final static String portal_file = "/opt/conf/portal.yml";

    private List<OneTopicRearrangeProcessor> threadList = new ArrayList<>();

    //每次最长取数据时间,单位秒,默认取5分钟
    public final static int MAX_FETCH_TIME_DURATION = 60;
    //每次最长取记录数
    public final static int MAX_FETCH_RECORDS = 100;

    //允许最慢topic和最慢topic之间的最大差值
    private long allowMaxAHeadSeconds=400;

    //////
    private long slowestThreadEventTime =0;

    //最慢topic线程Id
    private long slowestThreadIndex = -1;

    public void startAllThread() {

        KafkaConfigUtil kafkaConfigUtil = new KafkaConfigUtil();

        List<Map> topicConfigList = kafkaConfigUtil.getSourceTopicConfigList();
        Map sinkConfig = kafkaConfigUtil.getSinkConfig();
        int index = 0; //线程索引编号，从0开始
        for (Map topicConfig : topicConfigList) {
//            topicConfig.put("uri",kafkaConfigUtil.getUri());
//            topicConfig.put("auth",kafkaConfigUtil.isAuth());
//            topicConfig.put("user",kafkaConfigUtil.getUser());
//            topicConfig.put("password",kafkaConfigUtil.getPassword());
            sinkConfig.put("topic",topicConfig.get("topic").toString()+ "_rearranged");
            KafkaService kafkaService = new KafkaService(topicConfig,sinkConfig);


            OneTopicRearrangeProcessor thread = new OneTopicRearrangeProcessor(index++, this, kafkaService);
            threadList.add(thread);
            thread.start();
        }

    }

    public void caculateLowesThread(){
        OneTopicRearrangeProcessor thread = this.getSlowestThread();
        this.slowestThreadIndex = thread.getThreadIndex();
        this.slowestThreadEventTime=thread.getLatestEventTime();
    }

    public OneTopicRearrangeProcessor getSlowestThread(){
        OneTopicRearrangeProcessor[] threadArray = threadList.toArray(new OneTopicRearrangeProcessor[]{});

        for(int j=0;j<threadList.size()-1;j++){
           if(threadArray[j].getLatestEventTime()<threadArray[j+1].getLatestEventTime()) {
               OneTopicRearrangeProcessor temp = threadArray[j + 1];
               threadArray[j + 1] = threadArray[j];
               threadArray[j] = temp;
           }
       }
       return threadArray[threadArray.length-1];
    }

    //重启数据输入进程（由于该进程的处理的window过去超前，数据输入进程被暂停
    /*class ProcessorResumeThread extends Thread{

        public void run(){
            while(true){
                System.out.println("InputProcessorResumeThread:");

                for(OneTopicRearrangeProcessor processor:threadList){
                    //如果窗口数差值在允许范围内，则重新开始输入输入
                    if(processor.getLatestEventTime()-slowestThreadEventTime<allowMaxAHeadSeconds){
                        if(processor.getLatch().getCount()>0) {
                            processor.getLatch().countDown();
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
*/


    public long getSlowestThreadIndex() {
        return slowestThreadIndex;
    }

    public void setSlowestThreadIndex(long slowestThreadIndex) {
        this.slowestThreadIndex = slowestThreadIndex;
    }


    public long getSlowestThreadEventTime() {
        return slowestThreadEventTime;
    }

    public long getAllowMaxAHeadSeconds() {
        return allowMaxAHeadSeconds;
    }

    public static void  main(String[] args){
        new KafkaRearrangeMain().startAllThread();
    }
}


