package com.bingoyes.kafka.rearrange.manual;

import com.bingoyes.kafka.rearrange.manual.util.HttpRequest;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaRearrangeMain {

    private final static String portal_file = "/opt/conf/portal.yml";

    //允许最小窗口和最大窗口最大差值
    private long allowMaxAHeadWindowNum=10;

    private List<OneTopicRearrangeProcessor> threadList = new ArrayList<>();

    private List<String> topicList;

    //最慢的topic线程当前窗口标识(窗口开始时间）
    private long slowestThreadWindowId =0;

    private long slowestThreadIndex = -1;

    public void prepareKafkaConfig(){
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

            // read global main

            // read  dae-graph

        }catch(Exception e){
            logger.error("ams get redis config from portal.yml error, now trying to connect local redis");
            e.printStackTrace();

            //TODO
            hostName = "127.0.0.1";
            port =  6379;
            password = null;
        }
        if("".equals(hostName) || port == -1){
            logger.error("redis hot is empty, now trying to connect local redis");
            hostName = "127.0.0.1";
            port =  6379;
            password = null;
        }

        LettuceConnectionFactory lettuceConnectionFactory =
                createLettuceConnectionFactory
                        (cacheDatabaseIndex,hostName,port,password,maxIdle,minIdle,maxActive,maxWait,timeOut,shutdownTimeOut);
        return lettuceConnectionFactory;
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
        String result = HttpRequest.sendGet(url);
        Yaml yaml =new Yaml();


        if(result == null) {
            throw new Exception("");
        }else{
            logger.info("app config content:\n" +);

            try {
                Map<String, Object> contentProperties = yaml.load(result);

                Map mongo = (Map) contentProperties.get("mongo");
                Object uri = mongo.get("uri");
                Map security = (Map)mongo.get("security");
                auth = (Boolean) security.get("auth");
                if(auth) {
                    user = (String) security.get("user");
                    password = (String) security.get("pwd");
                }

                List<String> uriList = new ArrayList<>();
                if(uri instanceof  List){

                    uriList = (List)uri;
                }else{
                    uriList.add((String )uri);
                }

                String hostName = null;
                int port = -1;

                mongoServerList = new ArrayList<ServerAddress>();
                for(String oneUri:uriList) {
                    String[] tempList =oneUri.split(":");
                    hostName = tempList[0];
                    port = Integer.parseInt(tempList[1]);
                    mongoServerList.add(new ServerAddress(hostName,port));
                }

            } catch (Exception e) {
                logger.error("ams get mongo config  error,e");
                throw e;
            }
        }
    }

    public void startAllThread() {

        for (String topic : topicList) {
            KafkaSourceService kafkaSourceService = new KafkaSourceService(topic);
            KafkaSinkService kafkaSinkService = new KafkaSinkService(topic + "_rearranged");
            int index = 0; //线程Id
            OneTopicRearrangeProcessor thread = new OneTopicRearrangeProcessor(index++, this, kafkaSourceService, kafkaSinkService);
            threadList.add(thread);
            thread.start();
        }

    }

    public OneTopicRearrangeProcessor getSlowestThread(){
        OneTopicRearrangeProcessor[] threadArray = threadList.toArray(new OneTopicRearrangeProcessor[]{});

        for(int j=0;j<threadList.size()-1;j++){
           if(threadArray[j].getLatestProcessedWindowId()<threadArray[j+1].getLatestProcessedWindowId()) {
               OneTopicRearrangeProcessor temp = threadArray[j + 1];
               threadArray[j + 1] = threadArray[j];
               threadArray[j] = temp;
           }
       }
       return threadArray[threadArray.length-1];
    }

    public long getSlowestThreadWindowId() {
        return slowestThreadWindowId;
    }

    public void setSlowestThreadWindowId(long slowestThreadWindowId) {
        this.slowestThreadWindowId = slowestThreadWindowId;
    }

    public long getSlowestThreadIndex() {
        return slowestThreadIndex;
    }

    public void setSlowestThreadIndex(long slowestThreadIndex) {
        this.slowestThreadIndex = slowestThreadIndex;
    }

    public long getAllowMaxAHeadWindowNum() {
        return allowMaxAHeadWindowNum;
    }

    public void setAllowMaxAHeadWindowNum(long allowMaxAHeadWindowNum) {
        this.allowMaxAHeadWindowNum = allowMaxAHeadWindowNum;
    }
}


