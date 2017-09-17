package com.youyu.kafka;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by root on 2016/12/28.
 */
public class KafkaSendMsgApp extends KafkaService {
    public static void main(String[] args) throws InterruptedException {
        String topic = "bill_topic2";
        String kafkaBrokers = "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460";

        KafkaProducer producer = new KafkaProducer(kafkaBrokers);

        String sendMsg = null;
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String[] deviceId = {"device00", "device01", "device02", "device03", "device04", "device05", "device06",
                "device07", "device08", "device09", "device10"};
        String appKey = "appKey";
        String deviceType = "0";
        String deviceOs = "aaName";
        String deviceModel = "R9";
        String deviceBrand = "OPPO";
        String appVersion = "version";
        String appName = "youyu";
        String appSource = "12019";
        String appChannel = "channel";
        String appNetWork = "4G";
        String cityName = "SH";
        String userId = "userid";
        String userName = "userName";
        String[] enterTime = {"2017-05-08 00:23:23", "2017-05-08 01:23:23", "2017-05-08 02:23:23", "2017-05-08 " +
                "03:23:23", "2017-05-08 04:23:23", "2017-05-08 05:23:23", "2017-05-08 06:23:23", "2017-05-07 " +
                "00:23:23", "2017-05-07 01:23:23", "2017-05-07 02:23:23", "2017-05-07 " +
                "03:23:23", "2017-05-07 04:23:23", "2017-05-07 05:23:23", "2017-05-07 06:23:23"};

        for (int i = 1; i < 11; i++) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("{\"deviceId\":\"" + deviceId[i] + "\",");
            buffer.append("\"appKey\":\"" + appKey + i + "\",");
            buffer.append("\"deviceType\":\"" + deviceType + i + "\",");
            buffer.append("\"deviceOs\":\"" + deviceOs + i + "\",");
            buffer.append("\"deviceModel\":\"" + deviceModel + i + "\",");
            buffer.append("\"deviceBrand\":\"" + deviceBrand + i + "\",");
            buffer.append("\"appVersion\":\"" + appVersion + i + "\",");
            buffer.append("\"appName\":\"" + appName + i + "\",");
            buffer.append("\"appSource\":\"" + appSource + i + "\",");
            buffer.append("\"appChannel\":\"" + appChannel + i + "\",");
            buffer.append("\"appNetWork\":\"" + appNetWork + i + "\",");
            buffer.append("\"cityName\":\"" + cityName + i + "\",");
            buffer.append("\"userId\":\"" + userId + i + "\",");
            buffer.append("\"userName\":\"" + userName + i + "\",");
            buffer.append("\"enterTime\":\"" + enterTime[i] + "\"}");

            sendMsg = buffer.toString();
            System.out.println("正在发送消息------>" + sendMsg);

            //pushToTopic(topic, sendMsg);
            //producer.sendMessage(topic, sendMsg);

            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
