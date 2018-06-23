package com.lym.kafka.consumer;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * Created by LIUYANMIN on 2018/6/8.
 */
public class KafkaConsumerTest {

    public static void main(String[] args) throws IOException {
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        System.in.read();
    }

}
