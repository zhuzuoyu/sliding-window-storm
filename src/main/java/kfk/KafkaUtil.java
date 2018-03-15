package kfk;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zuoyuzhu on 2018/3/12.
 */
public class KafkaUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(KafkaUtil.class);

    public static void main(String[] args)
    {
        /*Producer producerThread = new Producer(KafkaProperties.topic);
        producerThread.start();*/

        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();

        LOG.info("tuichu ...");
    }
}
