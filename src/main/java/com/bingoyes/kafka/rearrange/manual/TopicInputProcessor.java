package com.bingoyes.kafka.rearrange.manual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class TopicInputProcessor extends Thread {

    private static Logger logger = LoggerFactory.getLogger(TopicInputProcessor.class);
    private int threadIndex;

    private KafkaRearrangeMain context;
    private long watermark;  //水位线，单位秒

    private long maxOutOfOrderness = 500; //单位秒

    private Map<Long, ProcessorWindow> windowHash = new HashMap<>();



    private KafkaSourceService kafkaSourceService;
    private KafkaSinkService kafkaSinkService;

    //生成的最新窗口的标识(开始时间)
    private long latestWindowId;

    //最后一个被处理window的标识(开始时间)
    private long latestProcessedWindowId;

    private CountDownLatch windowOutputLatch = new CountDownLatch(1);
    private CountDownLatch inputLatch = new CountDownLatch(0);


    public TopicInputProcessor(int threadIndex, KafkaRearrangeMain context, KafkaSourceService kafkaSourceService, KafkaSinkService kafkaSinkService){
        this.threadIndex = threadIndex;
        this.context = context;
        this.kafkaSourceService = kafkaSourceService;
        this.kafkaSinkService = kafkaSinkService;
    }

    @Override
    public void run(){

        new WindowProcessTriggerThread().start();

        while(true){
            try {
                inputLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            List<MessageRecord> recordList = kafkaSourceService.readMessage();
            System.out.println("thread:"+threadIndex+",get record size:"+recordList.size());
            for(MessageRecord record:recordList){
                assignWindowFor(record);
            }
        }
    }

    public void assignWindowFor(MessageRecord record){
        doAssignWindowFor(record);

        //todo 后更新watermark
        if(record.getTimestamp()>watermark) {

            watermark = record.getTimestamp();

           //watermark变化，触发窗口检查线程
           windowOutputLatch.countDown();
        }

    }

    public void doAssignWindowFor(MessageRecord messageRecord){
        System.out.println("thread:"+threadIndex+",assign window:"+messageRecord.getTimestamp());

        long timeZero = new Date(0).getTime();
        long recordTimestamp = messageRecord.getTimestamp();

        int windowSize = context.getWindowSize();
        long windowsStart = (recordTimestamp-timeZero)/windowSize*windowSize;

        ProcessorWindow processorWindow = getWindowById(windowsStart);
        if(processorWindow ==null) {
            System.out.println("thread:"+threadIndex+",create window:"+windowsStart);
            processorWindow = new ProcessorWindow(this, windowsStart, windowsStart + windowSize);

            //生成的最新窗口的Id
            latestWindowId = processorWindow.getStartTime();
            windowHash.put(windowsStart, processorWindow);
        }else{
            logger.info("thread:"+threadIndex+",get exist window:"+processorWindow.getStartTime());
        }

        processorWindow.addRecord(messageRecord);

    }

    public void pauseInputProcessor(){
        inputLatch = new CountDownLatch(1);
    }

    public void resumeInputProcessor(){
        inputLatch.countDown();
    }


    public ProcessorWindow getWindowById(long windowId){
        return windowHash.get(windowId);
    }


    class WindowProcessTriggerThread extends Thread{
        public void run(){
            while(true){
                try {
                    windowOutputLatch.await(); //触发端为：watermark新增
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println("thread:"+threadIndex+",WindowProcessTriggerThread run");

                Collection<ProcessorWindow> windowColl= windowHash.values();
                ProcessorWindow[] windowArray = windowColl.toArray(new ProcessorWindow[]{});
                System.out.println("thread:"+threadIndex+",window count:"+windowArray.length);
                for(int i= windowArray.length-1;i>=0;i--){
                    System.out.println("thread:"+threadIndex+",processing one window");
                    ProcessorWindow processorWindow = windowArray[i];

                    //触发处理当前Window的条件：1）水位线已经到达 2）和最慢线程额之间的差值在允许范围内
                    int windowSize =context.getWindowSize();
                    long lowestWindowId = context.getSlowestThreadLatestWindowId();
                    long aheadWindowNum = (processorWindow.getStartTime()-lowestWindowId)/windowSize;

                    System.out.println("thread:"+threadIndex+",watermark meets==========================:");
                    System.out.println("thread:"+threadIndex+",current window id:"+processorWindow.getStartTime());
                    System.out.println("thread:"+threadIndex+",lowest thread latest window id:"+context.getSlowestThreadLatestWindowId());
                    System.out.println("thread:"+threadIndex+",ahead window num:"+aheadWindowNum);
                    System.out.println("thread:"+threadIndex+",AllowMaxAHeadWindowNum:"+context.getAllowMaxAHeadWindowNum());

                    System.out.println("thread:"+threadIndex+",watermark:"+watermark);
                    System.out.println("thread:"+threadIndex+",window endtime:"+processorWindow.getEndTime());

                    //todo 线程之间的相差窗口数超出允许范围，说明本线程取数据和出处数据都快
                   if(watermark-maxOutOfOrderness>= processorWindow.getEndTime()){
                       //lowestWindowId==0标识第一次进入该分支代码
                       if(lowestWindowId!=0 && aheadWindowNum>context.getAllowMaxAHeadWindowNum()) {
                           System.out.println("thread:"+threadIndex+",not meets range in AllowMaxAHeadWindowNum!!!!!!!!!!!!!!!!!!!!!!!!:");
                           //暂停输入输入
                           pauseInputProcessor();
                           //if(watermark>= processorWindow.getEndTime() && aheadWindowNum<=context.getAllowMaxAHeadWindowNum()){
                       }else{

                            //线程之间的相差窗口数在允许范围内
                            System.out.println("thread:"+threadIndex+",meets range in AllowMaxAHeadWindowNum==========================:");
                            //恢复取数据线程
                            resumeInputProcessor();

                            //输出窗口数据
                            processorWindow.triggerSortAndOutput();

                           //更新最新处理的窗口
                           latestProcessedWindowId = processorWindow.getStartTime();

                            //如果当前线程是最慢的，需要更新最慢的线程的信息
                            //if (context.getSlowestThreadIndex() == threadIndex) {

                                //处理最慢窗口
                                context.workOnSlowestThread();
                            //}



                            //删除处理完成的widow
                            windowHash.remove(processorWindow.getStartTime());
                        }
                    }
                }

                //经过一个循环后，进入等待（watermark变化后会重新出发下一次循环）
                windowOutputLatch = new CountDownLatch(1);
            }
        }
    }

    public KafkaSinkService getKafkaSinkService() {
        return kafkaSinkService;
    }

    public void setKafkaSinkService(KafkaSinkService kafkaSinkService) {
        this.kafkaSinkService = kafkaSinkService;
    }

    public long getLatestProcessedWindowId() {
        return latestProcessedWindowId;
    }

    public int getThreadIndex() {
        return threadIndex;
    }

    public CountDownLatch getInputLatch() {
        return inputLatch;
    }
}
