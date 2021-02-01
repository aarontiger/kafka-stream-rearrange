package com.bingoyes.kafka.rearrange.manual;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaSinkService {
    private static Logger logger = LoggerFactory.getLogger(KafkaSinkService.class);

    Map topicConfig;

    String topic;

    public KafkaSinkService(Map topicConfig){
        this.topicConfig = topicConfig;
        init();
    }


    KafkaProducer kafkaProducer;
    private void init(){
        String uri = (String)topicConfig.get("uri");
        boolean auth = (boolean)topicConfig.get("auth");
        String password = (String)topicConfig.get("password");
        String groupId = (String)topicConfig.get("group_id");
        this.topic = (String)topicConfig.get("topic2");
        String user = (String)topicConfig.get("user");

        Properties props = new Properties();
        props.put("bootstrap.servers", uri);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if(auth) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + "admin" + "\" password=\"" + "admin" + "\";");
        }

        kafkaProducer = new KafkaProducer<>(props);
    }


    public void sendMessage(MessageRecord[] recordList){



        for(MessageRecord record:recordList) {

            String recordJson =  JSON.toJSONString(record);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,recordJson );

            //同步发送方式,get方法返回结果
            RecordMetadata metadata = null;
            try {
                metadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
                System.out.println("kafka output success:"+record.getTimestamp());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("broker返回消息发送信息" + metadata);
        }

    }
}
