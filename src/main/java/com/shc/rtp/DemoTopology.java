package com.shc.rtp;

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
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by uchaudh on 9/2/2015.
 */
public class DemoTopology {

    public static final Logger classLogger = LoggerFactory.getLogger(DemoTopology.class);

    public static int globalRecordCount = 0;
    public static StringBuilder sb=new StringBuilder();
    public static Properties props=new Properties();
    public static List<String> tupleList = new ArrayList<>();

    /**
     * Bolt Class
     */

//    TODO: Move this class in separate file
    public static class PrinterBolt extends BaseRichBolt {

        private static final long serialVersionUID = 1L;
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
               boltProps = loadPropertiesFromFile();
           }
           catch (Exception ex)
           {
               ex.printStackTrace();
           }
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                logger.info("Logging tuple with logger: " + tuple.toString() +"\n");
//                logger.info("Value of globalRecordCount: " + globalRecordCount+"\n");
                globalRecordCount++;
//                System.out.println("Received from Kafka : " + tuple.toString() + "\n");
                tupleList.add(tuple.toString());
//                sb.append(tuple.toString()+"\n");
                if(globalRecordCount==100) {
                    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");

//                    TODO: Resolve below HDFS write error: RPC.getProxy, so such method error !

//                    Configuration config = new Configuration();
//                    config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//                    config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//                    FileSystem fs = DistributedFileSystem.get(new URI(boltProps.getProperty("hdfs.namenode")), config);
//                    String fileName=boltProps.getProperty("hdfs.namenode")+boltProps.getProperty("hdfs.out.path")+"/1st_File_" +sdf.format(new Date())+".txt";
//                    Path filenamePath = new Path(fileName);
//                    if (!fs.exists(filenamePath)) {
//                        FSDataOutputStream fin = fs.create(filenamePath);
//                        fin.writeUTF(sb.toString());
//                        fin.close();
//                        sb=new StringBuilder();
//                        globalRecordCount=0;
//                        logger.info("Written in HDFS File  : " + fileName+"\n");
////                        fs.delete(filenamePath, true);
//                    }
//                    else {
//                        FSDataOutputStream fin = fs.append(filenamePath);
//                        fin.writeUTF(tuple.toString());
//                        fin.close();
//                    }
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

        props=loadPropertiesFromFile();

        String zkIp = props.getProperty("kafka.zookeeper.host");

        String nimbusHost = props.getProperty("storm.nimbus");

        String zookeeperHost = zkIp+ ":" + props.getProperty("kafka.zookeeper.port");

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, args[0], "", "spoutGrp_12");
//        kafkaConfig.startOffsetTime=kafka.api.OffsetRequest.EarliestTime();

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.zkServers=Arrays.asList(zkIp);
        kafkaConfig.zkPort=2181;
        kafkaConfig.zkRoot="";
//        kafkaConfig.metricsTimeBucketSizeInSecs=10;

        kafkaConfig.forceFromStart=false;

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafkaMessageConsumer", kafkaSpout, 1);

        builder.setBolt("kafkaMessageProcessor", new PrinterBolt(), 1)
                .shuffleGrouping("kafkaMessageConsumer");

        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);

        System.setProperty("storm.jar", props.getProperty("jar.file.path"));

        //More bolts stuffzz

        if (args != null && args.length > 1) {
            String name = args[1];
            String[] zkHostList= args[2].split(",");
            List<String> sl= Arrays.asList(zkHostList);
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(3);
            config.put(Config.NIMBUS_HOST, nimbusHost);
            config.put(Config.NIMBUS_THRIFT_PORT, 6628);
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
//            classLogger.info("From properties file is: "+host +" , " +user);


            String myUrl = "jdbc:mysql://172.29.81.1/npos";
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(myUrl, "root", "root");

            List<String> tuples= new ArrayList();
            for (String tupleStr: tupleList)
            {
                tuples.add("('"+tupleStr+"','"+ sdf.format(new Date()) +"' )");
            }

            String query=" insert into tran_kafkaBoltOut (data, crt_ts) values " + StringUtils.join(tuples,",");

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
