package com.lym.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

/**
 * Created by LIUYANMIN on 2018/6/8.
 */
public class KafkaConsumerServer implements MessageListener<String, String> {
    private static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerServer.class);

    @Override
    public void onMessage(ConsumerRecord<String, String> record) {
        LOGGER.info("=====================kafkaConsumer开始消费（日志开始）====================");
        String topic = record.topic();
        String key = record.key();
        String value = record.value();
        long offset = record.offset();
        int partition = record.partition();

        LOGGER.info("--------------topic: " + topic);
        LOGGER.info("--------------key: " + key);
        LOGGER.info("--------------value: " + value);
        LOGGER.info("--------------offset: " + offset);
        LOGGER.info("--------------partition: " + partition);
        LOGGER.info("=====================kafkaConsumer结束消费（日志结束）====================");
    }

}
