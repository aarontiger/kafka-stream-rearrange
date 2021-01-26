package com.bingoyes.kafka.rearrange.manual;

import java.util.List;

public class ProcessWindow {
    private long startTime;
    private long endTime;
    private List<MessageRecord> recordList;

    private MessageRecord currPointer;

    public ProcessWindow(long startTime,long endTime){
        this.startTime = startTime;
        this.endTime = endTime;
    }
    public void addRecord(MessageRecord record){

        recordList.add(record);

        //形成双向链表
//        if(currPointer!=null){
//            MessageRecord currRecord = currPointer;
//            MessageRecord nextRecord = currRecord.getNext();
//            MessageRecord preRecord = currRecord.getPrevious();
//            if(record.getTimestamp().getTime()>currRecord.getTimestamp().getTime()){
//                while(record.getTimestamp().getTime()>currRecord.getTimestamp().getTime()){
//                    currRecord =currRecord.getPrevious();
//                    //转了一圈
//                    if(currRecord==currPointer) break;
//
//                    //插入到currRecord前面
//                    record.setPrevious(currRecord.getPrevious());
//                    record.setNext(currRecord);
//
//                    currRecord.getPrevious().setNext(record);
//                    currRecord.setPrevious(record);
//                }
//            }else if(record.getTimestamp().getTime()<currRecord.getTimestamp().getTime()){
//                while(record.getTimestamp().getTime()<currRecord.getTimestamp().getTime()){
//                    currRecord =currRecord.getNext();
//                }
//            }
//        }
    }

    public void triggerProcess(){

    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
}
