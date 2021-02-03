package com.bingoyes.kafka.rearrange.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

public class KafkaConfigUtil {

    private static Logger logger = LoggerFactory.getLogger(KafkaConfigUtil.class);

    private final static String portal_file = "/opt/conf/portal.yml";
    private final static String kafka_file = "/opt/conf/kafka-stream.yml";

/*    String uri;
    boolean auth;
    String user;
    String password;*/

    List<Map> sourceTopicConfigList = new ArrayList<>();

    Map<String,Object> sinkConfig = new HashMap<>();

    public KafkaConfigUtil(){
        try {
            //readFormGlobalMain();
            //this.topicConfigList = readFormDaeGraph();
           readKafkaConfigFromFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Map> readKafkaConfigFromFile(){
        Yaml yaml =new Yaml();
        String uri=null;
        boolean auth = false;
        String user =null;
        String password =null;
        try{
            File file = new File(kafka_file);

            Map<String, Map<String, Map<String, Map>>> contentProperties = yaml.load(new FileInputStream(file));

            Map mongo = (Map) contentProperties.get("kafkasource");
            List<String> uriList = (List<String>)mongo.get("uri");
            if(uriList!=null & uriList.size()>0)
                uri = uriList.get(0);

            Map security = (Map)mongo.get("security");
            auth = (Boolean) security.get("auth");
            if(auth) {
                user = (String) security.get("user");
                password = (String) security.get("pwd");
            }

            Map<String,Map> kafkasourceTopics = (Map) contentProperties.get("kafkasource-topics");
            Set<String> keySet = kafkasourceTopics.keySet();
            List<Map>  valueList = new ArrayList<>(kafkasourceTopics.values());
            Map[] valueArray = kafkasourceTopics.values().toArray(new Map[]{});

            Map activeKafkaSource = new HashMap();

            //去掉未激活的topic
            for(String key:keySet){
                Map topicConf = kafkasourceTopics.get(key);
                boolean active = (boolean)topicConf.get("active");

                if(active){
                    Map value = kafkasourceTopics.get(key);
                    value.put("uri",uri);
                    value.put("auth",auth);
                    value.put("user",user);
                    value.put("password",password);
                    value.put("uri",uri);
                    activeKafkaSource.put(key,value);
                }
            }

            this.sourceTopicConfigList =new ArrayList<Map>(activeKafkaSource.values());

            //输出kafka
            String uri2=null;
            boolean auth2 = false;
            String user2 =null;
            String password2 =null;
            Map mongo2 = (Map) contentProperties.get("kafkasink");
            List<String> uriList2 = (List<String>)mongo2.get("uri");
            if(uriList!=null & uriList.size()>0)
                uri2 = uriList2.get(0);

            Map security2 = (Map)mongo.get("security");
            auth2 = (Boolean) security.get("auth");
            if(auth2) {
                user2 = (String) security.get("user");
                password2 = (String) security.get("pwd");
            }

            sinkConfig.put("uri",uri2);
            sinkConfig.put("auth",auth2);
            sinkConfig.put("user",user2);
            sinkConfig.put("password",password2);


        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
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




    public List<Map> getSourceTopicConfigList() {
        return sourceTopicConfigList;
    }

    public void setSourceTopicConfigList(List<Map> sourceTopicConfigList) {
        this.sourceTopicConfigList = sourceTopicConfigList;
    }

    public Map<String, Object> getSinkConfig() {
        return sinkConfig;
    }
}
