package slidingwindow;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//IRichSpout
public class SlidingSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingSpout.class);
    SpoutOutputCollector collector;

    private static final long serialVersionUID = 5028304756439810609L;


    int intsmaze = 0;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
    }


    @Override
    public void nextTuple() {
        // TODO Auto-generated method stub
        System.out.println("发送数据:" + intsmaze);
        intsmaze++;
        this.collector.emit(new Values(intsmaze));
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void ack(Object msgId) {
        // TODO Auto-generated method stub
        LOG.debug("Got ACK for msgId : 123");
    }

    @Override
    public void fail(Object msgId) {
        // TODO Auto-generated method stub
        LOG.debug("Got FAIL for msgId : 456");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


}
