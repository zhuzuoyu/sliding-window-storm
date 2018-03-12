package kfk;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * Created by zuoyuzhu on 2018/3/12.
 */
public class Producer extends Thread
{
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();
    public Producer(String topic)
    {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "10.32.32.112:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }
    @Override
    public void run() {
        int messageNo = 1;
        while (true)
        {
            String messageStr = new String("Message_" + new Date());
            System.out.println("Send:" + messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            messageNo++;
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}