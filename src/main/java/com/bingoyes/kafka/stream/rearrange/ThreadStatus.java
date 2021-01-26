package com.bingoyes.kafka.stream.rearrange;

import com.bingoyes.kafka.rearrange.manual.MessageRecord;

import java.util.Date;
import java.util.List;

public class ThreadStatus {
    private Date startTime;
    private Date endTime;
    private int status; //-1:新周期开始预备  0：周期进行中  1:本周期数据准备完毕 2：本周期数据输出完毕

    private List<MessageRecord> recordList;

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public List<MessageRecord> getRecordList() {
        return recordList;
    }

    public void setRecordList(List<MessageRecord> recordList) {
        this.recordList = recordList;
    }
}
