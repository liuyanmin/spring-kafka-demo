package com.lym.kafka.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

/**
 * Created by LIUYANMIN on 2018/6/8.
 */
public class KafkaProducerListener implements ProducerListener {
    private static Logger LOGGER = LoggerFactory.getLogger(KafkaProducerListener.class);

    /**
     * 消息发送成功后调用
     */
    @Override
    public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata) {
        LOGGER.info("=====================kafka发送数据成功（日志开始）====================");
        LOGGER.info("--------------topic: " + topic);
        LOGGER.info("--------------partition: " + partition);
        LOGGER.info("--------------key: " + key);
        LOGGER.info("--------------value: " + value);
        LOGGER.info("--------------recordMetadata: " + recordMetadata);
        LOGGER.info("=====================kafka发送数据成功（日志结束）====================");
    }

    /**
     * 消息发送失败后调用
     */
    @Override
    public void onError(String topic, Integer partition, Object key, Object value, Exception exception) {
        LOGGER.info("=====================kafka发送数据失败（日志开始）====================");
        LOGGER.info("--------------topic: " + topic);
        LOGGER.info("--------------partition: " + partition);
        LOGGER.info("--------------key: " + key);
        LOGGER.info("--------------value: " + value);
        LOGGER.info("--------------exception: " + exception);
        LOGGER.info("=====================kafka发送数据失败（日志结束）====================");
        exception.printStackTrace();
    }

    /**
     * 方法返回值表示是否启动kafkaProducer监听器
     */
    @Override
    public boolean isInterestedInSuccess() {
        LOGGER.info("启动KafkaProducer监听器");
        return true;
    }
}
