package com.shc.rtp.cassandra;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.shc.rtp.common.NPOSConfiguration;
import com.shc.rtp.enums.FieldEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by uchaudh on 10/16/2015.
 */
public class CassandraLoggerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 4449815947065569980L;
    private static ClusterConnectionManager cfdao = null;
    private OutputCollector collector;
    private static final Gson GSON = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(CassandraLoggerBolt.class);
    private static Session session =null;
    private static final NPOSConfiguration configuration = new NPOSConfiguration();
    private String kafka_topic=null;



    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        cfdao = new ClusterConnectionManager();
        cfdao.connect(configuration);
        session= cfdao.getSession();
        kafka_topic= configuration.getString("kafka.topic");

    }

    @Override
    public void execute(Tuple input) {
        try {
            String nposMessage= input.getString(0);
            System.out.println("Received Error Message from Bolt: " + nposMessage);
            String sqlString="INSERT INTO npos.kafkapublishererrorlog (uid,affected_kafka_topic,crt_ts,lupd_ts,message_string ,replayed) VALUES (uuid(),'"+kafka_topic+"',dateof(now()),dateof(now()) , '"+nposMessage+"', "+false+" )";
            session.execute(sqlString);
            this.collector.ack(input);
        } catch (Exception e) {
              LOG.error(e.getMessage());
//            final IErrorLogger errorLogger = ErrorLoggerFactory.getErrorLogger(nposMessage.getComponentFailureEnum());
//            final String jsonRep = GSON.toJson(nposMessage);
//            errorLogger.logMessage(jsonRep);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {
//        cfdao.cleanup();
    }
}
