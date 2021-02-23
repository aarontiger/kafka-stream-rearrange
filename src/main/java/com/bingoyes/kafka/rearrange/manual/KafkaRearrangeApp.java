package com.bingoyes.kafka.rearrange.manual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * GAT1400服务启动入口
 **/
@SpringBootApplication
@EnableScheduling
public class KafkaRearrangeApp implements CommandLineRunner
{

    private static Logger logger = LoggerFactory.getLogger(KafkaRearrangeApp.class);

    @Autowired
    KafkaRearrangeMain kafkaRearrangeMain;

    public static void main(String[] args) {
        SpringApplication.run(KafkaRearrangeApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        kafkaRearrangeMain.startAllThread();
    }
}