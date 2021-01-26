package com.bingoyes.kafka.rearrange.manual;

import java.util.Date;

public class MessageRecord {

    private long timestamp;
    private String domainGroup;

    private MessageRecord next;

    private MessageRecord previous;


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDomainGroup() {
        return domainGroup;
    }

    public void setDomainGroup(String domainGroup) {
        this.domainGroup = domainGroup;
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
