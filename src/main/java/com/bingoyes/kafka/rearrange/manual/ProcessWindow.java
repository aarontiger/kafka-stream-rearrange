package com.bingoyes.kafka.rearrange.manual;

import java.util.ArrayList;
import java.util.List;

public class ProcessWindow {
    private long startTime;
    private long endTime;
    private List<MessageRecord> recordList;

    private MessageRecord currPointer;

    private RearrangeThread context;

    public ProcessWindow(RearrangeThread context,long startTime,long endTime){
        this.context = context;
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

    /**
     * 该窗口的排序并输出
     */
    public void triggerSortAndOutput(){
        MessageRecord[] list = getOrderedRecordList(this.recordList);
        KafkaSinkService kafkaSinkService = context.getKafkaSinkService();
        kafkaSinkService.sendMessage(list);
    }

    public MessageRecord[] getOrderedRecordList(List<MessageRecord> list){
        List<MessageRecord> resultList = new ArrayList<>();
        MessageRecord[] listArray = list.toArray(new MessageRecord[]{});
        QuickSortUtil.quickSort(listArray);
        return listArray;
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
