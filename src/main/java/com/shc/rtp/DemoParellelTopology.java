package com.shc.rtp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Arrays;

/**
 * Created by uchaudh on 9/8/2015.
 */
public class DemoParellelTopology {

    public static final Logger LOG = LoggerFactory.getLogger(DemoParellelTopology.class);

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println("Recieved from Kafka : " + tuple.toString() + "\n");
        }
    }

    public static void main(String[] args) throws Exception {

        String zkIp = "172.29.80.75";

        String nimbusHost = "151.149.131.11";

        String zookeeperHost = "172.29.80.75:2181";

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "stormtest", "", "storm12");
//        kafkaConfig.startOffsetTime=kafka.api.OffsetRequest.EarliestTime();

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.zkServers= Arrays.asList(zkIp);
        kafkaConfig.zkPort=2181;
        kafkaConfig.zkRoot="";

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("eventsEmitter", kafkaSpout, 1);

        builder.setBolt("eventsProcessor", new PrinterBolt(), 1)
                .shuffleGrouping("eventsEmitter");

        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 500);

        System.setProperty("storm.jar", "C:\\Users\\uchaudh\\stormKafka\\out\\artifacts\\stormKafka_jar\\stormKafka.jar");

        //More bolts stuffzz

        if (args != null && args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            config.put(Config.NIMBUS_HOST, "151.149.131.11");
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
            StormSubmitter.submitTopology(name, config, builder.createTopology());
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-testing", config, builder.createTopology());
        }
    }
}
