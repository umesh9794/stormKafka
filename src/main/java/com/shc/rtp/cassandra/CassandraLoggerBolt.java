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
import com.shc.rtp.NPOSKafka.notification.model.NotificationModel;
import com.shc.rtp.common.NPOSConfiguration;
import com.shc.rtp.enums.FieldEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    private String topology_id=null;



    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        cfdao = new ClusterConnectionManager();
        cfdao.connect(configuration);
        session= cfdao.getSession();
        kafka_topic= configuration.getString("kafka.topic");
        topology_id=topologyContext.getThisComponentId();

    }

    @Override
    public void execute(Tuple input) {
        try {
            SimpleDateFormat date = new SimpleDateFormat("MM/dd/yyyy");
            SimpleDateFormat time = new SimpleDateFormat("h:mm:ss a");
            String nposMessage= input.getString(0);
            String cassandraTableName= input.getString(1);
            String sourceTopology= input.getString(2);
            String sqlString=null;
            NotificationModel notificationModel= (NotificationModel) input
                    .getValueByField(FieldEnum.FIELD_NOTIFICATION_DETAILS
                            .getFieldName());
            String errorMessage=notificationModel.getErrorDescription().replace("'","''");

             String  publisherTableName=configuration.getString("cassandra.publisher.tablename");

            if(cassandraTableName.equals(configuration.getString("cassandra.publisher.tablename"))) {
                sqlString = "INSERT INTO " + cassandraTableName + " (uid,affected_kafka_topic,crt_ts,lupd_ts,message_string ,replayed,error_message) VALUES (uuid(), '" + kafka_topic + "',dateof(now()),dateof(now()) , '" + nposMessage.replace("'","''") + "', " + false + ", '"+errorMessage +"' )";
            }
            else {
                sqlString = "INSERT INTO " + cassandraTableName + " (key ,error_date ,error_message ,event_time , npos_message  , replay_required_flag, retry_count,topology_id ) VALUES ( uuid(),'"+date.format(new Date())+"', '"+errorMessage+"' ,'"+time.format(new Date())+"' , '" + nposMessage.replace("'","''") + "', " + true + ", 0 ,'"+topology_id+"' )";
            }
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
