package com.youyu.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka生产者
 * 发送消息只需要kafka borkers地址即可
 */
@SuppressWarnings("unused")
public class KafkaProducer extends KafkaService{
    private String confKafkaBrokerList;
    /**
     * kafka生产者
     *
     * @param kafkaBrokerList brokers地址
     */
    public KafkaProducer(String kafkaBrokerList) {
        this.confKafkaBrokerList = kafkaBrokerList;
        logger.info("kafka.broker.list:" + this.confKafkaBrokerList);
    }

    /**
     * 发送消息
     *
     * @param topic   主题
     * @param message 消息内容
     * @return 是否发送成功
     */
    public boolean sendMessage(String topic, String message) {
        try {
            Properties proper = new Properties();
            proper.put("metadata.broker.list", this.confKafkaBrokerList);
            proper.put("serializer.class", "kafka.serializer.StringEncoder");
            proper.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            proper.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            proper.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(proper);
            Producer<String, String> producer = new Producer<>(config);
            KeyedMessage<String, String> data = new KeyedMessage<>(topic, message);
            producer.send(data);
            producer.close();
            return true;
        } catch (Exception err) {
            logger.warn("Kafka发送信息错误:" + err.toString());
            err.printStackTrace();
            return false;
        }
    }
}
