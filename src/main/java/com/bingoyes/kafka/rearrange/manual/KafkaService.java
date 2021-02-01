package com.bingoyes.kafka.rearrange.manual;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaService {

    private static Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private Map topicConfig;

    private String topic;


    KafkaConsumer<String, String> consumer;
    KafkaProducer kafkaProducer;

    List<String> sourceTopicList = new ArrayList<>();

    public KafkaService(Map topicConfig){
        this.topicConfig = topicConfig;
        initConsumer();
        initProducer();
    }

    private void initConsumer(){

        String uri = (String)topicConfig.get("uri");
        boolean auth = (boolean)topicConfig.get("auth");
        String user = (String)topicConfig.get("user");
        String password = (String)topicConfig.get("password");
        String groupId = (String)topicConfig.get("group_id");
        this.topic = (String)topicConfig.get("topic");

        Properties props = new Properties();
        props.put("bootstrap.servers", uri);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("auto.offset.reset", "latest");
        //props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 500);
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

    private void initProducer(){
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

    public List<MessageRecord> readMessage(int runSecond,long maxRecordNum){

        long startTime = new Date().getTime()/1000;

        List<ConsumerRecord<String, String>> list = new ArrayList<>();


        List<MessageRecord> recordList = new ArrayList<>();
        int count =0;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {

               System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

                MessageRecord messageRecord = new MessageRecord();

                JSONObject recordJson = JSON.parseObject(record.value());
                messageRecord.setTimestamp(recordJson.getLong("timestamp"));
                Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset(), "no metadata"));
                messageRecord.setCurrentOffsets(currentOffsets);
                recordList.add(messageRecord);

                count++;

            }
            long currentTime = new Date().getTime()/1000;

            //运行分钟
            if(count>maxRecordNum || currentTime-startTime>runSecond){
                break;
            }
        }

        //consumer.commitSync();
        return  recordList;
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
                logger.info("commit consumer offesets");
                //提交consumer
                consumer.commitAsync(record.getCurrentOffsets(), new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (null != exception){
                            logger.info("发送消息后提交consumer失败");
                            System.out.println(String.format("==== Commit failed for offsets %s, error:%s ====", offsets, exception.toString()));
                        }else {
                            logger.info("commit consumer offesets");
                            Map<TopicPartition, OffsetAndMetadata> currentOffsets = record.getCurrentOffsets();
                            Set<TopicPartition> keys = currentOffsets.keySet();
                            for (TopicPartition topicPartition : keys) {
                                logger.info("commit conumer partition:" + topicPartition.partition() + ",topic:" + topicPartition.topic() + "offset:" + currentOffsets.get(topicPartition).offset());
                            }
                        }
                    }
                });

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("发送kafka消息成功" + metadata);
        }

    }

    public String getTopic() {
        return topic;
    }
}
