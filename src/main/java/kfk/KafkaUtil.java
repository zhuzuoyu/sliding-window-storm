package kfk;


/**
 * Created by zuoyuzhu on 2018/3/12.
 */
public class KafkaUtil {
    public static void main(String[] args)
    {
        Producer producerThread = new Producer(KafkaProperties.topic);
        producerThread.start();

        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();
    }
}
