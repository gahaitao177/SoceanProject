package com.youyu.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaService {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    /*public static boolean pushToTopic(String params) {
        try {
            Properties proper = new Properties();
            // kafka 地址
            proper.setProperty("metadata.broker.list", "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460");
            proper.setProperty("request.required.acks", "1");
            proper.setProperty("serializer.class", "kafka.serializer.StringEncoder");
            ProducerConfig config = new ProducerConfig(proper);
            Producer<String, String> producer = new Producer<String, String>(config);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("", params);
            producer.send(data);
            producer.close();
        } catch (Exception e) {
            logger.error("Kafka发送信息错误:", e);
            return false;
        }
        logger.info("BaseImpl:pushToTopic----end");
        return true;
    }

    public static boolean pushToTopic(String topic, String params) {
        logger.info("BaseImpl:pushToTopic----start");
        try {
            Properties proper = new Properties();
            // kafka 地址
            proper.setProperty("metadata.broker.list", "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460");
            proper.setProperty("serializer.class", "kafka.serializer.StringEncoder");
            proper.setProperty("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(proper);
            Producer<String, String> producer = new Producer<String, String>(config);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, params);
            producer.send(data);
            producer.close();
        } catch (Exception e) {
            logger.error("Kafka发送信息错误:", e);
            return false;
        }
        logger.info("BaseImpl:pushToTopic----end");
        return true;
    }*/

}
