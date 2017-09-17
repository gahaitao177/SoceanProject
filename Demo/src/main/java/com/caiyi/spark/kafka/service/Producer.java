package com.caiyi.spark.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producer extends Thread {

    private final KafkaProducer producer;

    public Producer() {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460");
        props.put("request.required.acks", "1");
        this.producer = new KafkaProducer(props);
    }

    public KafkaProducer getProducer() {
        return producer;
    }

    @Override
    public void run() {
        Random random = new Random();
        String topicName = "bill_topic";

        String messageStr = null;

        String[] arr = {"zhangsan,1", "lisi,1", "wangwu,1", "xiaoming,1", "Tom,1", "Jack,1", "Lily,1", "Jonth,1",
                "Lenn,1", "Jenn,1", "Lunn,1", "Lili,1",};

        for (int i = 1; i < 11; i++) {

            messageStr = arr[random.nextInt(13)];

            producer.send(new ProducerRecord(topicName, messageStr));

            System.out.println("正在发送消息：" + messageStr);

            try {
                sleep(20);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }


    }
}  