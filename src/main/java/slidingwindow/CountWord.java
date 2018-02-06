package slidingwindow;


import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWord extends BaseRichBolt {
    // public class CountWord extends BaseWindowedBolt{

    private static final long serialVersionUID = -5283595260540124273L;

    public static final Logger LOG = LoggerFactory.getLogger(FilterBoltSliding.class);


    @Override
    public void execute(Tuple input) {
        // TODO Auto-generated method stub
        int sum = 0;

        System.out.print("一个窗口内的总值为");
        System.out.println(input);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub

    }

}
