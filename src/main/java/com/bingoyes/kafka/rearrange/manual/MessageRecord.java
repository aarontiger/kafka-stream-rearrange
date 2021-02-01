package com.bingoyes.kafka.rearrange.manual;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class MessageRecord {
    private String recordType;
    private String recoredId;

    private long timestamp;
    private String domainGroup;

    private Hashtable properties;

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();


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


    public Hashtable getProperties() {
        return properties;
    }

    public void setProperties(Hashtable properties) {
        this.properties = properties;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public String getRecoredId() {
        return recoredId;
    }

    public void setRecoredId(String recoredId) {
        this.recoredId = recoredId;
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    public void setCurrentOffsets(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.currentOffsets = currentOffsets;
    }
}
