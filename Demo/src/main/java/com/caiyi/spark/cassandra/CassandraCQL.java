package com.caiyi.spark.cassandra;

import com.caiyi.spark.conf.ConfigurationManager;
import com.caiyi.spark.constant.Constants;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.net.InetAddress;

/**
 * Created by root on 2016/12/9.
 */
public class CassandraCQL {

    Session session = null;

    String keySpaceName = ConfigurationManager.getProperty(Constants.CASSANDRA_KEYSPACE_NAME);
    Integer heartBeatInterval = ConfigurationManager.getInteger(Constants.CASSANDRA_HEARTBEAT_INTERVAL_SECONDS);
    Integer localCoreConnectNum = ConfigurationManager.getInteger(Constants.CASSANDRA_LOCAL_CORE_CONNECT_NUM);
    Integer localMaxConnectNum = ConfigurationManager.getInteger(Constants.CASSANDRA_LOCAL_MAX_CONNECT_NUM);
    Integer remoteCoreConnectNum = ConfigurationManager.getInteger(Constants.CASSANDRA_REMOTE_CORE_CONNECT_NUM);
    Integer remoteMaxConnectNum = ConfigurationManager.getInteger(Constants.CASSANDRA_REMOTE_MAX_CONNECT_NUM);
    Integer clusterPort = ConfigurationManager.getInteger(Constants.CASSANDRA_CLUSTER_PORT);


    public void insertBillCQL(String cql) {

        CassandraPoolOptions conf = new CassandraPoolOptions();
        conf.setHeartbeatIntervalSeconds(heartBeatInterval);
        conf.setLocalCoreConnectNum(localCoreConnectNum);
        conf.setLocalMaxConnectNum(localMaxConnectNum);
        conf.setRemoteCoreConnectNum(remoteCoreConnectNum);
        conf.setRemoteMaxConnectNum(remoteMaxConnectNum);
        conf.setPort(clusterPort);
        conf.getContactPoints().add((InetAddress) Constants.CASSANDRA_DB_ADDRESS);

        CassnadraClient client = new CassnadraClient(conf);

        try {
            session = client.connect(Constants.CASSANDRA_KEYSPACE_NAME);

            ResultSet rs = session.execute(cql);

            System.out.println("是否保存成功:" + rs.wasApplied());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }

    }
}
