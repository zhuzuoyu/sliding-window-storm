package slidingwindow;  import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
//import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; import java.util.concurrent.TimeUnit; public class StormGenericTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(StormGenericTopology.class); 	private final TopologyProperties topologyProperties; 	public StormGenericTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}

	public void runTopology() throws Exception{ 		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();

		switch (stormExecutionMode){
			case ("cluster"):
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case ("local"):
			default:
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				Thread.sleep(topologyProperties.getLocalTimeExecution());
				cluster.killTopology(topologyProperties.getTopologyName());
				cluster.shutdown();
				System.exit(0);
		}
	} 	private StormTopology buildTopology()
	{
		/*
		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		String kafkaTopic = topologyProperties.getKafkaTopic();
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "/storm/kafka/"+topologyProperties.getTopologyName(), kafkaTopic);
		kafkaConfig.forceFromStart = topologyProperties.isKafkaStartFromBeginning();
		*/ 		TopologyBuilder builder = new TopologyBuilder();
		BaseWindowedBolt bolt = new FilterBoltSliding()
				.withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS));

		/*
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), topologyProperties.getKafkaSpoutParallelism());
		builder.setBolt("FilterBolt", new FilterMessageBolt(), topologyProperties.getFilterBoltParallelism()).shuffleGrouping("KafkaSpout");
		builder.setBolt("TCPBolt", new TCPBolt(), topologyProperties.getTcpBoltParallelism()).shuffleGrouping("FilterBolt");
		*/ 		builder.setSpout("spout", new SlidingSpout(), 1);

		builder.setBolt("slidingwindowbolt", bolt,1).shuffleGrouping("spout");
		builder.setBolt("countwordbolt", new CountWord(),1).shuffleGrouping("slidingwindowbolt");
		return builder.createTopology();
	} 	public static void main(String[] args) throws Exception {
		try{
			String propertiesFile = "E:\\sws\\sliding-window-storm\\src\\main\\resources\\storm-siem-topology.config.properties"; //args[0];
			TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
			StormGenericTopology topology = new StormGenericTopology(topologyProperties);
			topology.runTopology();     System.out.println("test");
		}catch(Exception e){
			System.out.println("ERROR: "+e.toString());
		} 	}
}
