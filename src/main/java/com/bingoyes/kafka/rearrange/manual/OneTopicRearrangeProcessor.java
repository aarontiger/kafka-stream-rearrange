package com.bingoyes.kafka.rearrange.manual;

import java.util.*;

public class OneTopicRearrangeProcessor extends Thread {
    private int threadIndex;

    private KafkaRearrangeMain context;
    private long watermark;  //水位线，单位秒

    private long maxOutOfOrderness; //单位秒

    private Map<Long, ProcessorWindow> windowHash = new HashMap<>();

    private int windowSize=120; //单位秒

    private KafkaSourceService kafkaSourceService;
    private KafkaSinkService kafkaSinkService;

    //最后一个被处理window的key(开始时间)
    private long latestProcessedWindowId;



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
        }

    }

    public void doAssignWindowFor(MessageRecord messageRecord){
        long timeZero = new Date(0).getTime();
        long recordTimestamp = messageRecord.getTimestamp();

        long windowsStart = (recordTimestamp-timeZero)/windowSize*windowSize;

        ProcessorWindow processorWindow = getWindowById(windowsStart);
        if(processorWindow ==null) {
            processorWindow = new ProcessorWindow(this, windowsStart, windowsStart + windowSize);
            windowHash.put(windowsStart, processorWindow);
        }

        processorWindow.addRecord(messageRecord);

    }


    public ProcessorWindow getWindowById(long windowId){
        return windowHash.get(windowId);
    }

    class WindowProcessTriggerThread extends Thread{
        public void run(){
            while(true){
                Collection<ProcessorWindow> windowColl= windowHash.values();
                ProcessorWindow[] windowArray = windowColl.toArray(new ProcessorWindow[]{});
                for(int i= windowArray.length-1;i>=0;i--){
                    ProcessorWindow processorWindow = windowArray[i];

                    //触发处理当前Window的条件：1）水位线已经到达 2）和最慢线程额之间的差值在允许范围内
                    long aheadWindowNum = (processorWindow.getStartTime()-context.getSlowestThreadLatestWindowId())/windowSize;
                    if(watermark>= processorWindow.getEndTime()){
                    //if(watermark>= processorWindow.getEndTime() && aheadWindowNum<=context.getAllowMaxAHeadWindowNum()){
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
