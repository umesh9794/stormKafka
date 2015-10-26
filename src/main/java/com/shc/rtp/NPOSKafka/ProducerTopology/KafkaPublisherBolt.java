package com.shc.rtp.NPOSKafka.ProducerTopology;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import backtype.storm.metric.api.CountMetric;
import com.google.gson.Gson;
import com.ibm.jms.JMSMessage;
import com.shc.rtp.NPOSKafka.notification.model.NotificationModel;
import com.shc.rtp.common.NPOSConfiguration;
import com.shc.rtp.enums.ComponentFailureEnum;
import com.shc.rtp.enums.FieldEnum;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import org.apache.commons.lang.CharSetUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.http.util.EncodingUtils;
import org.joda.time.DateTime;
import shc.npos.parsers.MessageParser;

import com.google.common.reflect.TypeToken;
import javax.jms.BytesMessage;
import javax.jms.Message;

/**
 * Created by uchaudh on 9/10/2015.
 */
public class KafkaPublisherBolt extends BaseRichBolt {

    private Gson gson;
    private static final long serialVersionUID = -128031238218711051L;
    private OutputCollector collector;

    private static String topic = null;
    private static final Properties PROPS = new Properties();
    private static Producer<String, String> producer = null;
    transient CountMetric counter;
    private static final NPOSConfiguration configuration = new NPOSConfiguration();
    private static int messageCounter=0;
    private static String kafkaZKHost=null;
    private static int kafkaZKPort=0;


    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        PROPS.put("serializer.class", "kafka.serializer.StringEncoder");
        PROPS.put("metadata.broker.list", configuration.getString("metadeta.broker.list"));
        PROPS.put("request.required.acks", "0");
        producer = new Producer<String, String>(new ProducerConfig(PROPS));
        topic = configuration.getString("kafka.topic");
        kafkaZKHost = configuration.getString("kafka.zookeeper.host");
        kafkaZKPort= configuration.getInt("kafka.zookeeper.port");
//        checkAndCreateTopic();
        counter = new CountMetric();
        context.registerMetric("record_execute_count",counter,1);
    }

    public void execute(Tuple input) {
        String strMessage = null;
        String segmentsJson=null;
        Message posMsg = null;
            try {
                gson= new Gson();
                Object jmsMsg = input.getValueByField("npos_message");
                posMsg = ((JMSMessage) jmsMsg);
                strMessage = convertStreamToString(posMsg);
//                System.out.println("Recieved Message:"+ strMessage);

                @SuppressWarnings("rawtypes")
                Map parsedSegments = MessageParser.instance().parseMessage(strMessage);

                @SuppressWarnings("serial")
                Type nposMessageType = new TypeToken<Map<String, Map<String, String>>>() {
                }.getType();
                segmentsJson = gson.toJson(parsedSegments, nposMessageType);

                if(segmentsJson.equals(null))
                    System.out.println("JSON is NULL");

//                System.out.println("Parsed JSON : "+segmentsJson);

                if(messageCounter++ %100==0) {
                    throw new Exception("Manually Thrown Exception... Dont Worry :-)");
                }
                producer.send(new KeyedMessage<String, String>(topic, segmentsJson));
                this.collector.ack(input);

//                this.collector.emit(FieldEnum.FIELD_ERROR_MESSAGE.getFieldName(),new Values(input));//
//                System.out.println("Current Time Millis : "+ System.currentTimeMillis()+"\n");
//                counter.incr();
            } catch (Exception e) {
//                producer = null;
//                NPOSMessageDetail failedMessage = new NPOSMessageDetail(nposMessage.getTopologyID(), nposMessage.getNposMessage(),
//                        ExceptionUtils.getFullStackTrace(e), ComponentFailureEnum.KAFKA_BOLT, nposMessage.getRetryCount(), new Date());
//                this.collector.emit(new Values(failedMessage));
                NotificationModel notificationModel = new NotificationModel("KafkaPublisher",
                        e.getMessage(), new DateTime(), ComponentFailureEnum.KAFKA_DEMO_PUBLISHER_BOLT);
                this.collector.emit(new Values(segmentsJson,configuration.getString("cassandra.publisher.tablename"),"kafkaPublisher",notificationModel));
                this.collector.fail(input);
            }
        }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("npos_message","cassandra_table_name","source_topology",FieldEnum.FIELD_NOTIFICATION_DETAILS
                .getFieldName()));
    }

    /**
     *
     * @param jmsMsg
     * @return
     * @throws Exception
     */
    private static String convertStreamToString(final Message jmsMsg) throws Exception {
        String stringMessage = "";
        BytesMessage bMsg = (BytesMessage) jmsMsg;
        byte[] buffer = new byte[40620];
        int byteRead;
        ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
        while ((byteRead = bMsg.readBytes(buffer)) != -1) {
            bout.write(buffer, 0, byteRead);
        }
        bout.flush();
        stringMessage = new String(bout.toByteArray());
        bout.close();
        return stringMessage;
    }

    /**
     * Check for Kafka Topic existence and create if not exists
     */
    private static void checkAndCreateTopic()
    {
        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        Properties topicConfig = new Properties();
        ZkClient zkClient = new ZkClient(kafkaZKHost+":"+kafkaZKPort, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
        System.out.println("Topic Already Exists !");

        if(!AdminUtils.topicExists(zkClient,topic )){
            System.out.println("Topic Not Exists !");
            AdminUtils.createTopic(zkClient, topic, configuration.getInt("kafka.topic.partition"),configuration.getInt("kafka.topic.replication"),topicConfig);
            System.out.println("Kafka Topic "+ topic+" Created Successfully!");
        }
    }
}