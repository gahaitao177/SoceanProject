package com.youyu.kafka;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Iterator;

/**
 * Created by root on 2016/12/28.
 */
public class KafkaSendMsgAppProfile extends KafkaService {
    public static void main(String[] args) throws InterruptedException {
        String topic = "kafka_010_topic";
        String kafkaBrokers = "192.168.1.71:9164,192.168.1.73:9312,192.168.3.37:9383";

        KafkaProducer producer = new KafkaProducer(kafkaBrokers);

        String sendMsg = "{\"pkgId\":\"com.youyu.yystat\",\"appSource\":\"12027\"," +
                "\"appName\":\"\\u8bb0\\u8d26\\u8f6f\\u4ef6\",\"userId\":\"b2b21018-a735-48cd-82f2-035a27567c79\"," +
                "\"deviceRes\":\"1920*1080\",\"histories\":[{\"exitTime\":1494316769769," +
                "\"pageViewTimeLong\":\"1494316769\",\"enterTime\":-1,\"exitTimeStr\":\"2017-05-09 15:59:29\"," +
                "\"page\":\"com.caiyi.accounting.jz.MainActivity\",\"enterTimeStr\":\"\"}," +
                "{\"exitTime\":1494316776366,\"pageViewTimeLong\":\"0\",\"enterTime\":1494316776340," +
                "\"exitTimeStr\":\"2017-05-09 15:59:36\",\"page\":\"com.caiyi.accounting.jz.AddRecordActivity\"," +
                "\"enterTimeStr\":\"2017-05-09 15:59:36\"}],\"reportTime\":\"2017-05-24 01:59:38\",\"appGps\":\"\"," +
                "\"city\":\"\\u6c88\\u9633\\u5e02\",\"osVersion\":\"Android_23(6.0.1)\",\"deviceModel\":\"SM-A7108\"," +
                "\"appInstallPkgSource\":\"12027\",\"appVersion\":\"2.4.0\",\"deviceType\":\"android\"," +
                "\"events\":[{\"eventId\":\"main_add_record\",\"timeStr\":\"2017-05-09 15:59:29\"," +
                "\"time\":1494316769670},{\"eventId\":\"addRecord_type_in\",\"timeStr\":\"2017-05-09 15:59:31\"," +
                "\"time\":1494316771471},{\"eventId\":\"addRecord_save\",\"timeStr\":\"2017-05-09 15:59:36\"," +
                "\"time\":1494316776290},{\"eventId\":\"add_record_time_select\",\"timeStr\":\"2017-05-09 15:59:36\"," +
                "\"time\":1494316776294}],\"SimulateIDFA\":\"\",\"appKey\":\"yy_jz\",\"appIp\":\"218.24.67.115\"," +
                "\"deviceOs\":\"a7xeltecmcc\",\"mac\":\"02:00:00:00:00:00\",\"idfa\":\"\"," +
                "\"deviceId\":\"561b14c22cfe0f93\",\"androidId\":\"561b14c22cfe0f93\",\"imei\":\"\"," +
                "\"appNetWork\":\"4G\",\"openudid\":\"\",\"sdkVersion\":\"android-0.1.0_nolog\"," +
                "\"deviceBrand\":\"samsung\",\"country\":\"\\u4e2d\\u56fd\",\"region\":\"\\u8fbd\\u5b81\\u7701\"," +
                "\"clientId\":\"clientId006\"," +
                "\"clientIdMd5\":\"1c422011f7d5abdb9a69a23f0c09c467\",\"starts\":[],\"appChannel\":\"0001\"}";
        ;

        for (int i = 1; i < 2; i++) {

            System.out.println("正在发送消息------>" + sendMsg);

            //pushToTopic(topic, sendMsg);
            producer.sendMessage(topic, sendMsg);

            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
