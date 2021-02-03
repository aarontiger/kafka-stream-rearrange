package com.bingoyes.kafka.rearrange;

import com.bingoyes.kafka.rearrange.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
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

    private List<OneTopicRearrangeProcessor> threadList = new ArrayList<>();

    //每次最长取数据时间,单位秒,默认取5分钟
    public final static int MAX_FETCH_TIME_DURATION = 60;
    //每次最长取记录数
    public final static int MAX_FETCH_RECORDS = 10000;

    @Value("${max_fetch_time_duration}")
    private long maxFetchTimeDuration;
    @Value("${max_fetch_records}")
    private long maxFetchRecords;


    @Value("${enable_fence}")
    private boolean isEnableFence = false;
    //允许最慢topic和最慢topic之间的最大差值
    //private long allowMaxAHeadSeconds=400;
    //private long allowMaxAHeadSeconds=24*60*60;

    @Value("${allow_max_ahead}")
    private long allowMaxAHead =10*60*60;
    private long slowestThreadEventTime =-2;
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
            sinkConfig.put("topic",topicConfig.get("sink_topic").toString());
            KafkaService kafkaService = new KafkaService(topicConfig,sinkConfig);

            OneTopicRearrangeProcessor thread = new OneTopicRearrangeProcessor(index++, this, kafkaService);
            threadList.add(thread);
            thread.start();
        }
        if(isEnableFence) {
            new ProcessorResumeThread().start();
        }
    }

    public void caculateLowesThread(){
        logger.info("renew slowest thread info:");
        logger.info("old slowest thread:"+slowestThreadIndex);
        logger.info("old slowest thread eventTime:"+slowestThreadEventTime);
        OneTopicRearrangeProcessor thread = this.getSlowestThread();
        this.slowestThreadIndex = thread.getThreadIndex();
        this.slowestThreadEventTime=thread.getLatestEventTime();
        logger.info("new slowest thread:"+slowestThreadIndex);
        logger.info("new slowest thread eventTime:"+slowestThreadEventTime);
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
    class ProcessorResumeThread extends Thread{

        public void run(){
            while(true){
                logger.info("ProcessorResumeThread=====================:");

                boolean isAllThreadSuspend = true;
                //判断是否所有线程都挂起
                for(OneTopicRearrangeProcessor processor:threadList){
                    if(processor.getFenceLatch().getCount()==0){
                        isAllThreadSuspend = false;
                        break;
                    }
                }
                //让最慢的线程运行
                if(isAllThreadSuspend) {
                    OneTopicRearrangeProcessor slowestThread = getSlowestThread();
                    slowestThread.clearFenceLatch();

                    logger.info("all thread suspends,let one resume:" + slowestThread.getKafkaService().getSourceTopic());

                }else {
                    for (OneTopicRearrangeProcessor processor : threadList) {
                        //如果窗口数差值在允许范围内，则重新开始输入输入
                        if (processor.getLatestEventTime() - slowestThreadEventTime < allowMaxAHead) {
                            if (processor.getFenceLatch().getCount() > 0) {
                                processor.getFenceLatch().countDown();
                               logger.info("resume thread,topic:" + processor.getKafkaService().getSourceTopic());
                            }
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


    public long getSlowestThreadIndex() {
        return slowestThreadIndex;
    }

    public void setSlowestThreadIndex(long slowestThreadIndex) {
        this.slowestThreadIndex = slowestThreadIndex;
    }


    public long getSlowestThreadEventTime() {
        return slowestThreadEventTime;
    }

    public long getAllowMaxAHead() {
        return allowMaxAHead;
    }

    public boolean isEnableFence() {
        return isEnableFence;
    }

    public long getMaxFetchTimeDuration() {
        return maxFetchTimeDuration;
    }

    public void setMaxFetchTimeDuration(long maxFetchTimeDuration) {
        this.maxFetchTimeDuration = maxFetchTimeDuration;
    }

    public long getMaxFetchRecords() {
        return maxFetchRecords;
    }

    public void setMaxFetchRecords(long maxFetchRecords) {
        this.maxFetchRecords = maxFetchRecords;
    }
}


