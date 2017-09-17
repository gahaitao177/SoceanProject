package com.youyu.kafka;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by root on 2016/12/28.
 */
public class KafkaSendMsgBank extends KafkaService {
    public static void main(String[] args) throws InterruptedException {
        String topic = "bill_topic2";
        String kafkaBrokers = "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460";

        KafkaProducer producer = new KafkaProducer(kafkaBrokers);

        String sendMsg = null;
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String sessionid = "session";
        String actionType = "1";
        String bankCode = "nongyeBank";
        String userName = "aaName";
        String resultCode = "1";
        String resultDesc = "AA";

        for (int i = 1; i < 1001; i++) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("{\"sessionId\": \"" + sessionid + i + "\",");
            buffer.append("\"actionType\": \"" + actionType + i + "\",");
            buffer.append("\"bankCode\": \"" + bankCode + i + "\",");
            buffer.append("\"userName\": \"" + userName + i + "\",");
            buffer.append("\"resultCode\": \"" + resultCode + i + "\",");
            buffer.append("\"resultDesc\": \"" + resultDesc + i + "\",");
            buffer.append("\"requestDate\": \"" + dateFormat.format(new Date()) + "\"}");

            sendMsg = buffer.toString();
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
