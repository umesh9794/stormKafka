package com.shc.rtp.NPOSKafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.List;

/**
 * Created by uchaudh on 9/10/2015.
 */
public class NPOSKafkaPublisherTopology {

    public static void main(String[] args) throws Exception{

        String nimbusHost="172.29.80.75";
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("MQBrowserSpout",new MQBrowserSpout("STORM.QA.EES.DATACOLLECT.QC01"),2);
        topologyBuilder.setBolt("KafkaPublisherBolt",new KafkaPublisherBolt(),2).shuffleGrouping("MQBrowserSpout","mq_spout_msg_receive_success_stream");

        Config config = new Config();
        System.setProperty("storm.jar", "C:\\Users\\uchaudh\\stormKafka\\out\\artifacts\\stormKafka_jar\\stormKafka.jar");
        if (args != null && args.length > 1) {
            String name = args[1];
            String[] zkHostList= args[2].split(",");
            List<String> sl= Arrays.asList(zkHostList);
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(3);
            config.put(Config.NIMBUS_HOST, nimbusHost);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkHostList));
            StormSubmitter.submitTopology(name, config, topologyBuilder.createTopology());
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafkaPublisher-testing", config, topologyBuilder.createTopology());
        }



    }
}
