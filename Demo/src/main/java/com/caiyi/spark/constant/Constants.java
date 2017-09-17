package com.caiyi.spark.constant;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gaohaitao on 2016/11/28.
 */
public class Constants {

    //Cassandra
    public final static List<InetAddress> CASSANDRA_DB_ADDRESS = new ArrayList<InetAddress>() {{
        try {
            add(InetAddress.getByName("192.168.1.71"));
            add(InetAddress.getByName("192.168.1.80"));
            add(InetAddress.getByName("192.168.1.88"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }};

    public final static String CASSANDRA_KEYSPACE_NAME = "cassandra.keyspace.name";

    public final static String CASSANDRA_CLUSTER_PORT = "cassandra.cluster.port";

    public final static String CASSANDRA_LOCAL_CORE_CONNECT_NUM = "cassandra.local.core.connect.num";

    public final static String CASSANDRA_LOCAL_MAX_CONNECT_NUM = "cassandra.local.max.connect.num";

    public final static String CASSANDRA_REMOTE_CORE_CONNECT_NUM = "cassandra.remote.core.connect.num";

    public final static String CASSANDRA_REMOTE_MAX_CONNECT_NUM = "cassandra.remote.max.connect.num";

    public final static String CASSANDRA_HEARTBEAT_INTERVAL_SECONDS = "cassandra.heartbeat.interval.seconds";

    public final static String SPARK_APP_NAME = "CaiyiDailyAnalysis";

    public final static String ZOOKEEPER_LIST = "zookeeper.list";

    //Kafka
    public final static String KAFKA_TOPIC_NAME = "kafka.topic.name";

    public final static String KAFKA_GROUP_ID = "DefaultConsumerGroup";

    public final static String KAFKA_NUM_THREADS = "kafka.num.theads";
}
