package com.shc.rtp.NPOSKafka.ConsumerTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.shc.rtp.common.NPOSConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by uchaudh on 9/8/2015.
 */
public class DemoParellelTopology {

    public static final Logger LOG = LoggerFactory.getLogger(DemoParellelTopology.class);

    public static int globalRecordCount = 0;
    public static StringBuilder sb=new StringBuilder();
    public static List<String> tupleList = new ArrayList<>();
    private static final NPOSConfiguration configuration = new NPOSConfiguration();



    /**
     * Bolt Class
     */
    public static class PrinterBolt extends BaseRichBolt {

        private static final long serialVersionUID = 2L;
        private static final Logger logger = LoggerFactory.getLogger(PrinterBolt.class);
        private OutputCollector m_collector;
        private Properties boltProps=new Properties();

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map _map, TopologyContext _conetxt, OutputCollector _collector) {
           try {
               m_collector = _collector;
               boltProps=loadPropertiesFromFile();
           }
           catch (Exception ex)
           {
               ex.printStackTrace();
           }
        }

        @Override
        public void execute(Tuple tuple) {
            try {
//                logger.info("Logging tuple with logger: " + tuple.toString() +"\n");
//                logger.info("Value of globalRecordCount: " + globalRecordCount+"\n");
                globalRecordCount++;
//                System.out.println("Received from Kafka : " + tuple.toString() + "\n");
                tupleList.add(tuple.toString());
//                sb.append(tuple.toString()+"\n");
                if(globalRecordCount==100) {

                    insertIntoMysql();
                    tupleList.clear();
                    globalRecordCount=0;
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

        String zkIp = configuration.getString("kafka.zookeeper.host");

        String nimbusHost = configuration.getString("storm.nimbus");

        String zookeeperHost = zkIp+ ":" + configuration.getString("kafka.zookeeper.port");

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, args[0], "", "spoutGrp_18");
//        kafkaConfig.startOffsetTime=kafka.api.OffsetRequest.EarliestTime();

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.zkServers=Arrays.asList(zkIp);
        kafkaConfig.zkPort=2181;
        kafkaConfig.zkRoot="";

//        kafkaConfig.metricsTimeBucketSizeInSecs=10;

//        kafkaConfig.forceFromStart=false;

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        MQSenderBolt messageSender= new MQSenderBolt("hofmdsappstrs1.sears.com", 1414, "SQAT0263", "STORM.SVRCONN", "MDS0.STORM.RFID.PILOT.QL01");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafkaMessageConsumer", kafkaSpout, 2);

        builder.setBolt("kafkaMessageProcessor", messageSender, 2)
                .shuffleGrouping("kafkaMessageConsumer");


        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);

        System.setProperty("storm.jar", configuration.getString("jar.file.path"));

        //More bolts stuffzz

        if (args != null && args.length > 1) {
            String name = args[1];
            String[] zkHostList= args[2].split(",");
            List<String> sl= Arrays.asList(zkHostList);
            config.setNumWorkers(10);
            config.setMaxTaskParallelism(2);
            config.put(Config.NIMBUS_HOST, nimbusHost);
            config.put(Config.NIMBUS_THRIFT_PORT, 6628);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkHostList));
            StormSubmitter.submitTopology(name, config, builder.createTopology());
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-consumer-test", config, builder.createTopology());
        }
    }

    /**
     * Read properties file
     * @throws Exception
     */
    public static Properties loadPropertiesFromFile() throws Exception
    {
        final String resourceName = "application.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        }
        return props;
    }

    /**
     *
     */
    public static void insertIntoMysql()
    {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");

            //             TODO : Use Bolt's properties object to get below properties

//            String host=props.getProperty("mysql.host");
//            String user=props.getProperty("mysql.user");
//            String pass=props.getProperty("mysql.password");

            String myUrl = "jdbc:mysql://172.29.81.1/npos";
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(myUrl, "root", "root");

            List<String> tuples= new ArrayList();
            for (String tupleStr: tupleList)
            {
                tuples.add("('"+tupleStr+"','"+ sdf.format(new Date()) +"' )");
            }

            String query=" insert into tran_kafkaBoltOut_3 (data, crt_ts) values " + StringUtils.join(tuples, ",");

            PreparedStatement preparedStmt = conn.prepareStatement(query);
//            preparedStmt.setString(1, tuple.toString());
//            preparedStmt.setString(2, " ");

            // execute the prepared statement
            preparedStmt.execute();

            conn.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }

}

