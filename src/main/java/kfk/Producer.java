package kfk;

import com.alibaba.fastjson.JSONObject;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.*;

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
        Calendar calendar = Calendar.getInstance();
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(new Date());
        while (true)
        {
            calendar.setTime(new Date());
            calendar.get(Calendar.SECOND );
            String messageStr = new String("NEW------>Message_" + calendar.getTimeInMillis());
            System.out.println("Send:" + messageStr);
            //producer.send(new KeyedMessage<Integer, String>(topic, messageStr));



            HashMap<String,Object> obj = new HashMap<String,Object>();
            obj.put("a"+messageNo,"a"+messageNo);
            List<String> list = new ArrayList<>();
            list.add("tmp"+messageNo);
            obj.put("b"+messageNo,list);

            messageStr = JSONObject.toJSONString(obj);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            System.out.println("Send:" + messageStr);

            messageNo++;
            try {
                sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (calendar.getTimeInMillis()>calendar1.getTimeInMillis()+7000){
                System.out.println("完成输入队列...");
                break;
            }
        }
    }
}