package com.bingoyes.kafka.rearrange.manual;

import java.util.Date;

public class MessageRecord {

    private Date timestamp;



    private MessageRecord next;

    private MessageRecord previous;


    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public MessageRecord getNext() {
        return next;
    }

    public void setNext(MessageRecord next) {
        this.next = next;
    }

    public MessageRecord getPrevious() {
        return previous;
    }

    public void setPrevious(MessageRecord previous) {
        this.previous = previous;
    }
}
