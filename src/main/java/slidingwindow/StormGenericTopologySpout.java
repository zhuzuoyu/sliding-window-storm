package slidingwindow;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

//import org.apache.storm.shade.org.joda.time.Duration;

public class StormGenericTopologySpout {
    public static final Logger LOG = LoggerFactory
            .getLogger(StormGenericTopologySpout.class);
    private final TopologyProperties topologyProperties;

    public StormGenericTopologySpout(TopologyProperties topologyProperties) {
        this.topologyProperties = topologyProperties;
    }

    public void runTopology() throws Exception {
        StormTopology stormTopology = buildTopology();
        String stormExecutionMode = topologyProperties.getStormExecutionMode();

        switch (stormExecutionMode) {
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
    }

    private StormTopology buildTopology() {

		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		String kafkaTopic = topologyProperties.getKafkaTopic();
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "/storm/kafka/"+topologyProperties.getTopologyName(), kafkaTopic);
		//kafkaConfig.forceFromStart = topologyProperties.isKafkaStartFromBeginning();

        TopologyBuilder builder = new TopologyBuilder();
        /*BaseWindowedBolt bolt = new FilterBoltSliding()
                .withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS))*/;

		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), topologyProperties.getKafkaSpoutParallelism());
		builder.setBolt("FilterBolt", new FilterMessageBolt(),topologyProperties.getFilterBoltParallelism()).shuffleGrouping("KafkaSpout");
		builder.setBolt("TCPBolt", new TCPBolt(), topologyProperties.getTcpBoltParallelism()).shuffleGrouping("FilterBolt");
        /*builder.setSpout("spout", new SlidingSpout(), 1);

        builder.setBolt("slidingwindowbolt", bolt, 1).shuffleGrouping("spout");
        builder.setBolt("countwordbolt", new CountWord(), 1).shuffleGrouping("slidingwindowbolt");*/
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        try {
            String propertiesFile = "E:\\git\\SWS\\sliding-window-storm\\src\\main\\resources\\storm-siem-topology.config.properties"; //args[0];
            //propertiesFile = "/opt/tiefan/tmp/storm-siem-topology.config.properties";
            TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
            StormGenericTopologySpout topology = new StormGenericTopologySpout(topologyProperties);
            topology.runTopology();
            System.out.println("test");
        } catch (Exception e) {
            System.out.println("ERROR: " + e.toString());
        }
    }
}
