package slidingwindow;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;



public class slidingSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 5028304756439810609L;  
	
	private SpoutOutputCollector collector;  
	int intsmaze=0;
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		System.out.println("发送数据:"+intsmaze);
		
		collector.emit(new Values(intsmaze++));
		 try {
			 Thread.sleep(2000);
		 }catch (InterruptedException e) {
			 e.printStackTrace();
		 }
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("intsmaze"));
	}


}
