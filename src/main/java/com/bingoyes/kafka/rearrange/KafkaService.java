package com.bingoyes.kafka.rearrange;

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

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaService {

    private static Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private Map sourceTopicConfig;
    private Map sinkTopicConfig;

    private String sourceTopic;
    private String sinkTopic;


    KafkaConsumer<String, String> consumer;
    KafkaProducer kafkaProducer;

    List<String> sourceTopicList = new ArrayList<>();

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public KafkaService(Map sourceTopicConfig,Map sinkTopicConfig){
        this.sourceTopicConfig = sourceTopicConfig;
        this.sinkTopicConfig = sinkTopicConfig;
        initConsumer();
        initProducer();
    }

    private void initConsumer(){

        String uri = (String) sourceTopicConfig.get("uri");
        boolean auth = (boolean) sourceTopicConfig.get("auth");
        String user = (String) sourceTopicConfig.get("user");
        String password = (String) sourceTopicConfig.get("password");
        String groupId = (String) sourceTopicConfig.get("group_id");
        this.sourceTopic = (String) sourceTopicConfig.get("topic");
        boolean startEarliest =(boolean)sourceTopicConfig.get("start_earliest");

        Properties props = new Properties();
        props.put("bootstrap.servers", uri);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if(startEarliest) {
            props.put("auto.offset.reset", "earliest");
        }else{
            props.put("auto.offset.reset", "latest");
        }
        props.put("max.poll.records", KafkaRearrangeMain.MAX_FETCH_RECORDS);
        props.put("auto.commit.interval.ms", 1000*600);



        if(auth) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + user + "\" password=\"" +password + "\";");
        }

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(sourceTopic));
    }

    private void initProducer(){
        String uri = (String) sinkTopicConfig.get("uri");
        boolean auth = (boolean) sinkTopicConfig.get("auth");
        String password = (String) sinkTopicConfig.get("password");
        String groupId = (String) sinkTopicConfig.get("group_id");
        this.sinkTopic = (String) sinkTopicConfig.get("topic");
        String user = (String) sinkTopicConfig.get("user");

        Properties props = new Properties();
        props.put("bootstrap.servers", uri);
        //props.put("group.id", groupId);
        //props.put("enable.auto.commit", "false");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if(auth) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + user + "\" password=\"" + password + "\";");
        }

        kafkaProducer = new KafkaProducer<>(props);
    }

    public List<MessageRecord> readMessage(){
        return readMessage(KafkaRearrangeMain.MAX_FETCH_TIME_DURATION,KafkaRearrangeMain.MAX_FETCH_RECORDS);
    }

    public List<MessageRecord> readMessage(long runSecond,long maxRecordNum){

        long startTime = new Date().getTime()/1000;

        List<ConsumerRecord<String, String>> list = new ArrayList<>();


        List<MessageRecord> recordList = new ArrayList<>();
        int count =0;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {

               //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

                MessageRecord messageRecord = new MessageRecord();

                JSONObject recordJson = JSON.parseObject(record.value());
                messageRecord.setTimestamp(recordJson.getLong("timestamp"));
                messageRecord.setDomainGroup(recordJson.getString("domainGroup"));
                messageRecord.setRecordType(sourceTopic);
                String recordId ="";
                if("bingoyes-imsi".equals(sourceTopic)){
                    recordId= recordJson.getString("imsi");
                }else if("bingoyes-wifimac".equals(sourceTopic)){
                    recordId= recordJson.getString("mac");
                }else if("bingoyes-face".equals(sourceTopic)){
                    recordId= recordJson.getString("label");
                }else if("bingoyes-plate".equals(sourceTopic)){
                    recordId= recordJson.getString("plate");
                }else if("bingoyes-etc".equals(sourceTopic)){
                    recordId= recordJson.getString("obuId");
                }
                messageRecord.setRecoredId(recordId);

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


    private KafkaConsumer getCommitConsumer(){

        String uri = (String) sourceTopicConfig.get("uri");
        boolean auth = (boolean) sourceTopicConfig.get("auth");
        String user = (String) sourceTopicConfig.get("user");
        String password = (String) sourceTopicConfig.get("password");
        String groupId = (String) sourceTopicConfig.get("group_id");
        this.sourceTopic = (String) sourceTopicConfig.get("topic");

        Properties props = new Properties();
        props.put("bootstrap.servers", uri);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("auto.offset.reset", "latest");
        //props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 100);
        props.put("auto.commit.interval.ms", 1000*600);



        if(auth) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + user + "\" password=\"" +password + "\";");
        }

        KafkaConsumer consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public void sendMessage(MessageRecord[] recordList){


        for(MessageRecord record:recordList) {

            JSONObject recordJson =  (JSONObject)JSON.toJSON(record);
            if("bingoyes-imsi".equals(sourceTopic)){
                recordJson.put("imsi",record.getRecoredId());
            }else if("bingoyes-wifimac".equals(sourceTopic)){
                recordJson.put("mac",record.getRecoredId());
            }else if("bingoyes-face".equals(sourceTopic)){
                recordJson.put("label",record.getRecoredId());
            }else if("bingoyes-plate".equals(sourceTopic)){
                recordJson.put("plate",record.getRecoredId());
            }else if("bingoyes-etc".equals(sourceTopic)){
                recordJson.put("obuId",record.getRecoredId());
            }

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(sinkTopic,recordJson.toJSONString() );

            //同步发送方式,get方法返回结果
            RecordMetadata metadata = null;
            try {
                metadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
                //logger.info("kafka output success,topic:"+sinkTopic+",timestamp:"+dateFormat.format(new Date(record.getTimestamp()*1000)));

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }

    }

    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets){
        KafkaConsumer commitConsumer = getCommitConsumer();
        //提交consumer
        /*this.consumer.commitAsync(offsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (null != exception){
                    logger.info("commit consumer offset 失败");
                    System.out.println(String.format("==== Commit failed for offsets %s, error:%s ====", offsets, exception.toString()));
                }else {
                    logger.info("commit consumer offset 成功");

                }
                logger.info("offset list:");

                Set<TopicPartition> keys = offsets.keySet();
                for (TopicPartition topicPartition : keys) {
                    logger.info("partition:" + topicPartition.partition() + ",topic:" + topicPartition.topic() + "offset:" + offsets.get(topicPartition).offset());
                }
            }
        });*/
        this.consumer.commitSync(offsets);
        logger.info("commit success");
        Set<TopicPartition> keys = offsets.keySet();
        for (TopicPartition topicPartition : keys) {
            logger.info("commit info,partition:" + topicPartition.partition() + ",topic:" + topicPartition.topic() + ",offset:" + offsets.get(topicPartition).offset());
        }
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }
}
