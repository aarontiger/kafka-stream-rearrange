package com.bingoyes.kafka.rearrange.manual;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class OneTopicRearrangeProcessor extends Thread {
    private int threadIndex;

    private KafkaRearrangeMain context;
    private long watermark;  //水位线，单位秒

    private long maxOutOfOrderness = 120; //单位秒

    private Map<Long, ProcessorWindow> windowHash = new HashMap<>();

    private int windowSize=120; //单位秒

    private KafkaSourceService kafkaSourceService;
    private KafkaSinkService kafkaSinkService;

    //最后一个被处理window的key(开始时间)
    private long latestProcessedWindowId;

    private CountDownLatch prepareLatch= new CountDownLatch(1);
    private CountDownLatch processLatch= new CountDownLatch(1);



    public OneTopicRearrangeProcessor(int threadIndex, KafkaRearrangeMain context, KafkaSourceService kafkaSourceService, KafkaSinkService kafkaSinkService){
        this.threadIndex = threadIndex;
        this.context = context;
        this.kafkaSourceService = kafkaSourceService;
        this.kafkaSinkService = kafkaSinkService;
    }

    @Override
    public void run(){

        new WindowProcessTriggerThread().start();

        while(true){

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
           processLatch.countDown();
        }

    }

    public void doAssignWindowFor(MessageRecord messageRecord){
        System.out.println("thread:"+threadIndex+",assign window:"+messageRecord.getTimestamp());

        long timeZero = new Date(0).getTime();
        long recordTimestamp = messageRecord.getTimestamp();

        long windowsStart = (recordTimestamp-timeZero)/windowSize*windowSize;

        ProcessorWindow processorWindow = getWindowById(windowsStart);
        if(processorWindow ==null) {
            System.out.println("thread:"+threadIndex+",create window:"+windowsStart);
            processorWindow = new ProcessorWindow(this, windowsStart, windowsStart + windowSize);
            windowHash.put(windowsStart, processorWindow);
        }else{
            System.out.println("thread:"+threadIndex+",get exist window:"+processorWindow.getStartTime());
        }

        processorWindow.addRecord(messageRecord);

    }


    public ProcessorWindow getWindowById(long windowId){
        return windowHash.get(windowId);
    }

    class WindowProcessTriggerThread extends Thread{
        public void run(){
            while(true){
                try {
                    processLatch.await();
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
                    long aheadWindowNum = (processorWindow.getStartTime()-context.getSlowestThreadLatestWindowId())/windowSize;
                    System.out.println("thread:"+threadIndex+",watermark:"+watermark);
                    System.out.println("thread:"+threadIndex+",window endtime:"+processorWindow.getEndTime());
                    if(watermark-maxOutOfOrderness>= processorWindow.getEndTime()){
                    //if(watermark>= processorWindow.getEndTime() && aheadWindowNum<=context.getAllowMaxAHeadWindowNum()){
                        System.out.println("thread:"+threadIndex+",watermark meets==========================:");
                        processorWindow.triggerSortAndOutput();

                        //如果当前线程是最慢的，需要更新最慢的线程的信息
                        if(context.getSlowestThreadIndex() ==threadIndex){
                            //确定最慢窗口
                           OneTopicRearrangeProcessor slowestThread = context.getSlowestThread();
                           context.setSlowestThreadIndex(slowestThread.getThreadIndex());
                           context.setSlowestThreadLatestWindowId(slowestThread.getLatestProcessedWindowId());
                        }


                        //更新最新处理的窗口
                        latestProcessedWindowId = processorWindow.getStartTime();

                        //删除处理完成的widow
                        windowHash.remove(processorWindow.getStartTime());
                    }
                }
                processLatch = new CountDownLatch(1);
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
}
