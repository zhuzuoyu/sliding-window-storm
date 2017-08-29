package slidingwindow;



import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.metric.LoggingMetricsConsumer;

public class TopologyProperties {
	
	private String kafkaTopic;
	private String topologyName;
	private int localTimeExecution, kafkaSpoutParallelism, filterBoltParallelism, tcpBoltParallelism;
	private Config stormConfig;
	private String zookeeperHosts;
	private String stormExecutionMode;
	private boolean kafkaStartFromBeginning;
	private String jsonFilter;

	public TopologyProperties(String fileName){
		
		stormConfig = new Config();

		try {
			setProperties(fileName);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private Properties readPropertiesFile(String fileName) throws Exception{
		Properties properties = new Properties();
		FileInputStream in = new FileInputStream(fileName);
		properties.load(in);
		in.close();
		return properties;		
	}
	
	private void setProperties(String fileName) throws Exception{
		
		Properties properties = readPropertiesFile(fileName);
		topologyName = properties.getProperty("storm.topology.name","defaultTopologyName");
		localTimeExecution = Integer.parseInt(properties.getProperty("storm.local.execution.time","20000"));
		kafkaSpoutParallelism = Integer.parseInt(properties.getProperty("kafka.spout.paralellism","1"));
		filterBoltParallelism = Integer.parseInt(properties.getProperty("filter.bolt.paralellism","1"));
		tcpBoltParallelism = Integer.parseInt(properties.getProperty("tcp.bolt.paralellism","1"));
		
		/**
		kafkaTopic = properties.getProperty("kafka.topic");
		if (kafkaTopic == null)
			throw new ConfigurationException("Kafka topic must be specified in topology properties file");
			
		kafkaStartFromBeginning = new Boolean(properties.getProperty("kafka.startFromBeginning","false"));
		**/
		setStormConfig(properties);
	}

	@SuppressWarnings("deprecation")
	private void setStormConfig(Properties properties) throws ConfigurationException
	{

		Iterator it = properties.keySet().iterator();
		while (it.hasNext()) {
			String key = (String)it.next();
			String value = (String)properties.get(key);
			
			stormConfig.put(key, value);
		}
		stormExecutionMode = properties.getProperty("storm.execution.mode","local");
		int stormWorkersNumber = Integer.parseInt(properties.getProperty("storm.workers.number","1"));
		int maxTaskParallism = Integer.parseInt(properties.getProperty("storm.max.task.parallelism","2"));

		zookeeperHosts = properties.getProperty("zookeeper.hosts");
		if (zookeeperHosts == null){
			throw new ConfigurationException("Zookeeper hosts must be specified in configuration file");
		}
		
		int topologyBatchEmitMillis = Integer.parseInt(
				properties.getProperty("storm.topology.batch.interval.miliseconds","2000"));
		String nimbusHost = properties.getProperty("storm.nimbus.host","localhost");
		String nimbusPort = properties.getProperty("storm.nimbus.port","6627");
		
		// How often a batch can be emitted in a Trident topology.
		stormConfig.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, topologyBatchEmitMillis);
		stormConfig.setNumWorkers(stormWorkersNumber);
		stormConfig.setMaxTaskParallelism(maxTaskParallism);
		// Storm cluster specific properties
		stormConfig.put(Config.NIMBUS_HOST, nimbusHost);
		stormConfig.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(nimbusPort));
		stormConfig.put(Config.STORM_ZOOKEEPER_PORT, parseZkPort(zookeeperHosts));
		stormConfig.put(Config.STORM_ZOOKEEPER_SERVERS, parseZkHosts(zookeeperHosts));
		
		stormConfig.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		stormConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		stormConfig.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		
		

		
        // register metric consumer
        //stormConfig.registerMetricsConsumer(JMXMetricConsumer.class, 1);
        stormConfig.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
	}

	private static int parseZkPort(String zkNodes) 
	{
		String[] hostsAndPorts = zkNodes.split(",");
		int port = Integer.parseInt(hostsAndPorts[0].split(":")[1]);
		return port;
	}
	
	private static List<String> parseZkHosts(String zkNodes) {

		String[] hostsAndPorts = zkNodes.split(",");
		List<String> hosts = new ArrayList<String>(hostsAndPorts.length);

		for (int i = 0; i < hostsAndPorts.length; i++) {
			hosts.add(i, hostsAndPorts[i].split(":")[0]);
		}
		return hosts;
	}
	
	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public int getLocalTimeExecution() {
		return localTimeExecution;
	}

	public Config getStormConfig() {
		return stormConfig;
	}
	
	public String getZookeeperHosts() {
		return zookeeperHosts;
	}
	
	public String getStormExecutionMode() {
		return stormExecutionMode;
	}	
	
	public boolean isKafkaStartFromBeginning() {
		return kafkaStartFromBeginning;
	}

	public int getKafkaSpoutParallelism() {
		return kafkaSpoutParallelism;
	}

	public int getFilterBoltParallelism() {
		return filterBoltParallelism;
	}

	public int getTcpBoltParallelism() {
		return tcpBoltParallelism;
	}	
}
