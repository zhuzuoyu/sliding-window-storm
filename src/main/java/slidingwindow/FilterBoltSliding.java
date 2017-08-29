package slidingwindow;


import java.util.Map;

import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



//BaseRichBolt
public class FilterBoltSliding extends BaseWindowedBolt{
	

	private static final long serialVersionUID = 1L;
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
		
		System.out.print("一个窗口内的数据");
		
		for(Tuple tuple: ((Window<Tuple>) input).get()){			
			int str=(Integer) tuple.getValueByField("intsmaze");
			System.out.print(" "+str);
			 sum+=str;			
		}
		System.out.println("======="+sum);
		
		collector.emit(new Values(sum));
	}

	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("count"));
	}

	public void cleanup() {

	}




	
}
