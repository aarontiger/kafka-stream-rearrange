package com.bingoyes.kafka.rearrange.manual.util;

import com.bingoyes.kafka.rearrange.manual.MessageRecord;
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

public class KafkaConfigUtil {

    private static Logger logger = LoggerFactory.getLogger(KafkaConfigUtil.class);

    private final static String portal_file = "/opt/conf/portal.yml";

    String uri;
    boolean auth;
    String user;
    String password;

    List<Map> topicConfigList = new ArrayList<>();

    public KafkaConfigUtil(){
        try {
            readFormGlobalMain();
            this.topicConfigList = readFormDaeGraph();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





    public String  getAmsUrlPrefix(){
        Yaml yaml =new Yaml();
        String password = null;
        String hostName = null;
        int port = -1;

        //get redis config from file
        try{
            File file = new File(portal_file);

            Map<String, Map<String, Map<String, Map>>> o = yaml.load(new FileInputStream(file));
            Map ams = (Map) o.get("ams");
            Map mqboroker = (Map) ams.get("duic");
            String amsUri = mqboroker.get("uri").toString();

            return  amsUri;

        }catch(Exception e){

            e.printStackTrace();


        }
        return null;

    }

    private  void readFormGlobalMain() throws Exception {

        String url = getAmsUrlPrefix()+"/apps/global/main";
        String result = HttpRequest.sendGet(url,"");
        Yaml yaml =new Yaml();


        if(result == null) {
            throw new Exception("");
        }else{
            logger.info("get config from ams global main:\n" +result);

            try {
                Map<String, Object> contentProperties = yaml.load(result);

                Map mongo = (Map) contentProperties.get("kafka");
                List<String> uriList = (List<String>)mongo.get("uri");
                if(uriList!=null & uriList.size()>0)
                    uri = uriList.get(0);

                Map security = (Map)mongo.get("security");
                auth = (Boolean) security.get("auth");
                if(auth) {
                    user = (String) security.get("user");
                    password = (String) security.get("pwd");
                }

            } catch (Exception e) {
                logger.error("ams get mongo config  error,e");
                throw e;
            }
        }
    }

    public  List<Map> readFormDaeGraph() throws Exception {

        String url = getAmsUrlPrefix()+"/apps/dae/graph";
        String result = HttpRequest.sendGet(url,"");
        Yaml yaml =new Yaml();


        if(result == null) {
            throw new Exception("");
        }else{
            logger.info("get config from ams dae graph:\n" +result);

            try {
                Map<String, Object> contentProperties = yaml.load(result);

                Map<String,Map> kafkasource = (Map) contentProperties.get("kafkasource");
                Set<String> keySet = kafkasource.keySet();
                List<Map>  valueList = new ArrayList<>(kafkasource.values());
                Map[] valueArray = kafkasource.values().toArray(new Map[]{});

                Map activeKafkaSource = new HashMap();

                //去掉未激活的topic
                for(String key:keySet){
                    Map topicConf = kafkasource.get(key);
                    boolean active = (boolean)topicConf.get("active");
                    if(active) activeKafkaSource.put(key,kafkasource.get(key));
                }

                return new ArrayList<Map>(activeKafkaSource.values());

            } catch (Exception e) {
                logger.error("ams get mongo config  error,e");
                throw e;
            }
        }
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public boolean isAuth() {
        return auth;
    }

    public void setAuth(boolean auth) {
        this.auth = auth;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<Map> getTopicConfigList() {
        return topicConfigList;
    }

    public void setTopicConfigList(List<Map> topicConfigList) {
        this.topicConfigList = topicConfigList;
    }
}
