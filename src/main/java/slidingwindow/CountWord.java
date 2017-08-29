package slidingwindow;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class CountWord extends BaseWindowedBolt{
	

	private static final long serialVersionUID = -5283595260540124273L;
	private OutputCollector collector;

	public static final Logger LOG = LoggerFactory.getLogger(FilterBoltSliding.class);

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOG.debug("prepare ok");
	}


	
	public void execute(TupleWindow input) {
		// TODO Auto-generated method stub
		int sum=0;
		
		System.out.print("一个窗口内的总值为");
		
		for(Tuple tuple: ((Window<Tuple>) input).get()){			
			int i=(Integer) tuple.getValueByField("count");
			System.out.println("接收到一个bolt的总和值为:"+i);
			sum+=i;
		}
		
		System.out.println("一个窗口内的总值为:"+sum);
		
	}

	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("count"));
	}

	public void cleanup() {

	}

}
