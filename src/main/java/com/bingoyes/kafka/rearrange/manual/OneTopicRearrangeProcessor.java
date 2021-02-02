package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.QuickSortUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class OneTopicRearrangeProcessor extends Thread {
    private static Logger logger = LoggerFactory.getLogger(OneTopicRearrangeProcessor.class);

    private int threadIndex;

    private KafkaRearrangeMain context;


    private KafkaService kafkaService;

    private CountDownLatch fenceLatch = new CountDownLatch(0);
    private long latestEventTime = -1;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public OneTopicRearrangeProcessor(int threadIndex, KafkaRearrangeMain context, KafkaService kafkaService){
        this.threadIndex = threadIndex;
        this.context = context;
        this.kafkaService = kafkaService;
    }

    @Override
    public void run(){

        while(true){

            try {
                fenceLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            List<MessageRecord> recordList = kafkaService.readMessage(KafkaRearrangeMain.MAX_FETCH_TIME_DURATION,KafkaRearrangeMain.MAX_FETCH_RECORDS);
            System.out.println("thread:"+threadIndex+",get record size:"+recordList.size());

            if(recordList.size()>0) {

                MessageRecord[] listArray = recordList.toArray(new MessageRecord[]{});

                QuickSortUtil.quickSort(listArray);
                logger.info("thread:"+threadIndex+",sort success");
                for(MessageRecord messageRecord:listArray){
                    logger.info("topic:"+kafkaService.getSourceTopic()+"timestamp:"+ dateFormat.format(new Date(messageRecord.getTimestamp()*1000)));
                }
                kafkaService.sendMessage(listArray);
                logger.info("thread:"+threadIndex+",send message success");

                //commit
                Map<TopicPartition, OffsetAndMetadata>  maxOffset = recordList.get(recordList.size()-1).currentOffsets;
                kafkaService.commit(maxOffset);

                //栅栏处理
                if(context.isEnableFence()) {

                    this.latestEventTime = listArray[listArray.length - 1].getTimestamp();
                    logger.info("update lastEventTime,thread:" + threadIndex + ",timestamp:"+dateFormat.format(new Date(latestEventTime*1000)));
                    long slowestThreadEventTime = context.getSlowestThreadEventTime();

                    //slowesThreadEventTime ==-2 表示 第一次执行下面的代码，最慢线程还没有初始值
                    if (slowestThreadEventTime!=-2 && latestEventTime - slowestThreadEventTime > context.getAllowMaxAHeadSeconds()) {

                        setupFenceLatch();
                        logger.info("thread:" + threadIndex + ",too quick and suspend");
                    }

                    context.caculateLowesThread();
                }
            }
        }
    }

    public int getThreadIndex() {
        return threadIndex;
    }

    public long getLatestEventTime() {
        return latestEventTime;
    }

    public void clearFenceLatch() {
        fenceLatch.countDown();
    }

    public void setupFenceLatch(){
        fenceLatch = new CountDownLatch(1);
    }

    public CountDownLatch getFenceLatch() {
        return fenceLatch;
    }
}
