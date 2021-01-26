package com.bingoyes.kafka.rearrange.manual;

import java.util.*;

public class RearrangeThread extends Thread {

    private long watermark;

    private long maxOutOfOrderness;

    private Map<Long,ProcessWindow> windowList = new HashMap<>();

    private int windowSize=1;

    private KafkaService kafkaService;

    //最后一个被处理window的key(开始时间)
    private long latestProcessedWindow;

    public RearrangeThread(KafkaService kafkaService){
        this.kafkaService = kafkaService;
    }

    public Date getCurrentWatermark(){
        return null;
    }

    @Override
    public void run(){

        while(true){

            List<MessageRecord> recordList = kafkaService.readMessage();
            for(MessageRecord record:recordList){
                processRecord(record);
            }
        }
    }

    public void processRecord(MessageRecord record){
        if(record.getTimestamp().getTime()/1000>watermark) {

            watermark = record.getTimestamp().getTime() / 1000;
        }
        relateWithWindow(record);
    }

    public void relateWithWindow(MessageRecord messageRecord){
        long timeZero = new Date(0).getTime();
        long recordTimestamp = messageRecord.getTimestamp().getTime();

        long windowsStart = (recordTimestamp-timeZero)/windowSize*windowSize;

        ProcessWindow processWindow = getWindow(windowsStart);
        if(processWindow==null) {
            processWindow = new ProcessWindow(windowsStart, windowsStart + windowSize);
            windowList.put(windowsStart, processWindow);
        }
    }


    public ProcessWindow getWindow(long windowStart){
        return windowList.get(windowStart);
    }



    class WindowProcessTriggerThread extends Thread{
        public void run(){
            while(true){
                Collection<ProcessWindow> windowColl=windowList.values();
                ProcessWindow[] windowArray = windowColl.toArray(new ProcessWindow[]{});
                for(int i= windowArray.length-1;i>=0;i--){
                    ProcessWindow processWindow = windowArray[i];
                    if(watermark>=processWindow.getEndTime()){
                        processWindow.triggerProcess();
                        windowList.remove(processWindow.getStartTime());
                    }
                }
            }
        }
    }
}
