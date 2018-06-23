package com.lym.kafka.producer;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

/**
 * Created by LIUYANMIN on 2018/6/8.
 */
public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        KafkaProducerServer producerServer = (KafkaProducerServer) context.getBean("kafkaProducerServer");
        String topic = "topic-test";
        String value = "Hello Kafka lalala.";
        String ifPartition = "0";
        Integer partitionNum = 4;
        String role = "k";// 用来生成key
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> res = producerServer.sendMsgForTemplate(topic, value, ifPartition, partitionNum, role+i);

            System.out.println("测试结果如下：======================");
            String message = (String) res.get("message");
            String code = (String) res.get("code");

            System.out.println("code: " + code);
            System.out.println("message: " + message);
            Thread.sleep(100);
        }

    }

}
