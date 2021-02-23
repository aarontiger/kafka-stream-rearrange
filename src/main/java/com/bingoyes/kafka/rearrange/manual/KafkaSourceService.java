package com.bingoyes.kafka.rearrange.manual;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bingoyes.kafka.rearrange.manual.util.HttpRequest;
import com.fasterxml.jackson.databind.util.JSONPObject;
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
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaSourceService {

    private static Logger logger = LoggerFactory.getLogger(KafkaSourceService.class);

    private Map topicConfig;

    private String topic;


    KafkaConsumer<String, String> consumer;

    List<String> sourceTopicList = new ArrayList<>();

    public KafkaSourceService(Map topicConfig){
        this.topicConfig = topicConfig;
        init();
    }

    private void init(){

        String uri = (String)topicConfig.get("uri");
        boolean auth = (boolean)topicConfig.get("auth");
        String user = (String)topicConfig.get("user");
        String password = (String)topicConfig.get("password");
        String groupId = (String)topicConfig.get("group_id");
        this.topic = (String)topicConfig.get("topic");

        Properties props = new Properties();
        props.put("bootstrap.servers", uri);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("auto.offset.reset", "latest");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 50);
        props.put("auto.commit.interval.ms", 1000*600);



        if(auth) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + user + "\" password=\"" +password + "\";");
        }

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));


    }
    public List<MessageRecord> readMessage(){

        List<ConsumerRecord<String, String>> list = new ArrayList<>();


        List<MessageRecord> recordList = new ArrayList<>();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            MessageRecord messageRecord = new MessageRecord();

            JSONObject recordJson =JSON.parseObject(record.value());
            messageRecord.setTimestamp(recordJson.getLong("timestamp"));
            recordList.add(messageRecord);

        }

        //consumer.commitSync();
        return  recordList;
    }

    public void sendMessage(String topic,List<MessageRecord> recordList){
        Properties kafkaPropertie = new Properties();
        //配置broker地址，配置多个容错
        kafkaPropertie.put("bootstrap.servers", "192.168.90.20:9092");
        //配置key-value允许使用参数化类型
        kafkaPropertie.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPropertie.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(kafkaPropertie);
        for(MessageRecord record:recordList) {
            ProducerRecord<String, MessageRecord> producerRecord = new ProducerRecord<String, MessageRecord>(topic,record );

            //同步发送方式,get方法返回结果
            RecordMetadata metadata = null;
            try {
                metadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("broker返回消息发送信息" + metadata);
        }

    }

    public String getTopic() {
        return topic;
    }
}
