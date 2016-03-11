package com.shc.rtp.NPOSKafka.ProducerTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import com.shc.rtp.NPOSKafka.notification.NotificationEvaluatorBolt;
import com.shc.rtp.NPOSKafka.notification.NotificationSenderBolt;
import com.shc.rtp.cassandra.CassandraLoggerBolt;
import com.shc.rtp.common.NPOSConfiguration;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import com.shc.rtp.enums.FieldEnum;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by uchaudh on 9/10/2015.
 */
public class NPOSKafkaPublisherTopology {


    private static final NPOSConfiguration configuration = new NPOSConfiguration();

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String nimbusHost = configuration.getString("storm.nimbus");
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        WebsphereMQSpout wmqs=new WebsphereMQSpout("hfqamqsvr3.vm.itg.corp.us.shldcorp.com",1414,"SQCT0001","STORM.SVRCONN","STORM.QA.EES.DATACOLLECT.QC01");

        topologyBuilder.setSpout("MQBrowserSpout", wmqs, 1);
        topologyBuilder.setBolt("KafkaPublisherBolt", new KafkaPublisherBolt(), 2).shuffleGrouping("MQBrowserSpout", "mq_spout_msg_receive_success_stream");
        topologyBuilder.setBolt("CassandraBolt", new CassandraLoggerBolt(),2).shuffleGrouping("KafkaPublisherBolt");

        topologyBuilder.setBolt("notificationEval",new NotificationEvaluatorBolt(),2).shuffleGrouping("KafkaPublisherBolt");
        topologyBuilder.setBolt("notificationSend",new NotificationSenderBolt("umesh.chaudhary@searshc.com"),2).shuffleGrouping("notificationEval");


        Config config = new Config();
        //Comment below line while building JAR
        System.setProperty("storm.jar", configuration.getString("jar.file.path"));
        if (args != null && args.length > 5) {
            String name = args[1];
            String[] zkHostList = args[2].split(",");
            List<String> sl = Arrays.asList(zkHostList);
//            config.setNumWorkers(2);
//            config.setMaxTaskParallelism(3);
            config.put(Config.NIMBUS_HOST, nimbusHost);
            config.put(Config.NIMBUS_THRIFT_PORT, 6628);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkHostList));
            config.setNumAckers(20);
            config.setNumWorkers(20);
            config.setMessageTimeoutSecs(300);
            config.setStatsSampleRate(1.0);
            config.setMaxSpoutPending(50000);
            config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
            config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
            config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
            config.registerMetricsConsumer(LoggingMetricsConsumer.class,2);
            StormSubmitter.submitTopology(name, config, topologyBuilder.createTopology());
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafkaPublisher-test", config, topologyBuilder.createTopology());
        }
    }
}
