package kfk;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;

import java.util.*;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Created by zuoyuzhu on 2018/3/12.
 */
public class Consumer extends Thread
{
    private final ConsumerConnector consumer;
    private final String topic;
    public Consumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }
    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(new Date());

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            calendar.setTime(new Date());
            String message =new String(it.next().message());
            Map<String, Object> params = JSONObject.parseObject(message, new TypeReference<Map<String, Object>>(){});

            System.out.println("receive：" + message);
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            /*if (calendar.getTimeInMillis()>=calendar1.getTimeInMillis()+8000){
                System.out.println("完成消费队列...");
                break;
            }*/
        }
        System.out.println("temp");

    }
}