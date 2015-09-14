package com.shc.rtp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by uchaudh on 9/2/2015.
 */
public class DemoTopology {

    public static final Logger LOG = LoggerFactory.getLogger(DemoTopology.class);

    public static String hdfsNamemode= "";
    public static int globalRecordCount = 0;
    public static StringBuilder sb=new StringBuilder();

    /**
     * Bolt Class
     */
    public static class PrinterBolt extends BaseRichBolt {

        private static final long serialVersionUID = 1L;
        private static final Logger logger = LoggerFactory.getLogger(PrinterBolt.class);
        private OutputCollector m_collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map _map, TopologyContext _conetxt, OutputCollector _collector) {
            m_collector = _collector;
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                logger.info("Logging tuple with logger: " + tuple.toString() +"\n");
//                logger.info("Value of globalRecordCount: " + globalRecordCount+"\n");
                globalRecordCount++;
                System.out.println("Received from Kafka : " + tuple.toString() + "\n");
                sb.append(tuple.toString()+"\n");
                if(globalRecordCount==50) {
                    Configuration config = new Configuration();
                    config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                    config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

                    FileSystem fs = DistributedFileSystem.get(new URI("hdfs://inpunpc310350:9000"), config);
                    String fileName="hdfs://inpunpc310350:9000" + "/user/uchaudh/testTopology"+UUID.randomUUID().toString()+".txt";
                    Path filenamePath = new Path(fileName);
                    if (!fs.exists(filenamePath)) {
                        logger.info("HDFS File Name : " + fileName+"\n");
                        FSDataOutputStream fin = fs.create(filenamePath);
                        fin.writeUTF(sb.toString());
                        fin.close();
                        sb=new StringBuilder();
                        globalRecordCount=0;
//                        fs.delete(filenamePath, true);
                    }
//                    else {
//                        FSDataOutputStream fin = fs.append(filenamePath);
//                        fin.writeUTF(tuple.toString());
//                        fin.close();
//                    }
                }
            }
            catch (Exception ioe)
            {
                ioe.printStackTrace();
            }
            m_collector.ack(tuple);
        }
        }

    public static void main(String[] args) throws Exception {

        hdfsNamemode= args[0];

        String zkIp = "trqaeahdidat04.vm.itg.corp.us.shldcorp.com";

//        String nimbusHost = "172.29.80.75";

        String nimbusHost = "172.29.80.75";

        String zookeeperHost = "trqaeahdidat04.vm.itg.corp.us.shldcorp.com:2181";

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, args[3], "", "spoutGrp_3");
//        kafkaConfig.startOffsetTime=kafka.api.OffsetRequest.EarliestTime();

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.zkServers=Arrays.asList(zkIp);
        kafkaConfig.zkPort=2181;
        kafkaConfig.zkRoot="";
//        kafkaConfig.metricsTimeBucketSizeInSecs=10;

        kafkaConfig.forceFromStart=false;

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafkaMessageEmitter", kafkaSpout, 1);

        builder.setBolt("kafkaMessageProcessor", new PrinterBolt(), 1)
                .shuffleGrouping("kafkaMessageEmitter");

        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 500);

        System.setProperty("storm.jar", "C:\\Users\\uchaudh\\stormKafka\\out\\artifacts\\stormKafka_jar\\stormKafka.jar");

        //More bolts stuffzz

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
            StormSubmitter.submitTopology(name, config, builder.createTopology());
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-testing", config, builder.createTopology());
        }
    }
}
