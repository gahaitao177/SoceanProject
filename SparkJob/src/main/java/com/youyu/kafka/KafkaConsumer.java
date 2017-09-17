package com.youyu.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Mario on 2016/11/30 0030.
 * kafka消费者
 * 消费消息只需要zookeeper地址即可
 * kafka.consumer 自动从zk读取kafka集群的节点地址,并自动维护各主题offset,存储到zkConnString/consumers下
 */
@SuppressWarnings("unused")
public class KafkaConsumer extends KafkaService {

    private String zkConnString;
    private ConsumerConnector consumer;

    /**
     * kafka消费者
     *
     * @param zkConnString zookeeper地址,附带根路径
     */
    public KafkaConsumer(String zkConnString) {
        this.zkConnString = zkConnString;
        logger.info("zookeeper.connect:" + this.zkConnString);
    }

    /**
     * 获取单个主题的数据流
     * streamsNum 等于 主题分区数（最优）
     * streamsNum 大于 主题分区数时,多余的stream不会收到任何信息
     * streamsNum 小于 主题分区数时,kafka会自动合并多个分区消息至同一个stream
     * 如streamsNum > 1, 务必确保每个stream都有独立的线程处理
     * 分区数最好是streamNum的整数倍,如某主题有24个分区,streamsNum可以是24,12,8,6,...
     *
     * @param topic      主题名
     * @param groupId    组名
     * @param streamsNum 数据流数目
     * @return 数据流list
     */
    public List<KafkaStream<byte[], byte[]>> getKafkaStreams(String topic, String groupId, Integer streamsNum) {
        Properties props = new Properties();
        props.put("zookeeper.connect", this.zkConnString);
        props.put("group.id", groupId);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, streamsNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        return consumerMap.get(topic);
    }

    /**
     * 关闭Kafka消费者
     */
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
    }

}
