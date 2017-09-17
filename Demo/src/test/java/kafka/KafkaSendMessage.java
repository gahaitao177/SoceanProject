package kafka;


import com.caiyi.spark.kafka.service.KafkaService;
import com.caiyi.spark.kafka.service.Producer;

/**
 * Created by root on 2016/12/28.
 */
public class KafkaSendMessage extends KafkaService {
    public static void main(String[] args) throws InterruptedException {
        Producer producer = new Producer();
        producer.run();
    }
}
