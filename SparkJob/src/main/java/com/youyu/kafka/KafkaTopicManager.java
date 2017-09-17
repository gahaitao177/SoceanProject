package com.youyu.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * Created by Mario on 2016/11/30 0030.
 * kafka主题管理类
 * kafka主题全部存储于zookeeper,默认位于zkConnString/brokers/topics
 */
@SuppressWarnings("unused")
public class KafkaTopicManager extends KafkaService {

    private String zkConnString;
    private ZkClient zk;

    /**
     * kafka主题管理
     *
     * @param zkConnString zookeeper地址,附带根路径
     */
    public KafkaTopicManager(String zkConnString) {
        this.zkConnString = zkConnString;
        logger.info("zookeeper.connect:" + this.zkConnString);
        zk = new ZkClient(
                this.zkConnString,
                6000,
                6000,
                ZKStringSerializer$.MODULE$
        );
    }

    /**
     * 创建主题
     *
     * @param topic             主题名
     * @param partitions        分区数
     * @param replicationFactor 副本数
     */
    public void createTopic(String topic, Integer partitions, Integer replicationFactor) {
        if(!AdminUtils.topicExists(zk,topic)){
            AdminUtils.createTopic(zk, topic, partitions, replicationFactor, new Properties());
        }else{
            logger.info(topic + " 主题已存在");
        }
    }

    /**
     * 删除主题
     *
     * @param topic 主题名
     */
    public void deleteTopic(String topic) {
        AdminUtils.deleteTopic(zk, topic);
    }
}
