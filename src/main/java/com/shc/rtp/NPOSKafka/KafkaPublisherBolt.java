package com.shc.rtp.NPOSKafka;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;
import com.ibm.jms.JMSMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

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
//    private static final NPOSConfiguration configuration = new NPOSConfiguration();

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        PROPS.put("serializer.class", "kafka.serializer.StringEncoder");
        PROPS.put("metadata.broker.list", "trqaeahdidat04.vm.itg.corp.us.shldcorp.com:9092,trqaeahdidat05.vm.itg.corp.us.shldcorp.com:9092");
        PROPS.put("request.required.acks", "0");
        producer = new Producer<String, String>(new ProducerConfig(PROPS));
        topic = "shc.rtp.npos";
    }

    public void execute(Tuple input) {
        String strMessage = null;
        Message posMsg = null;
            try {
                gson= new Gson();
                Object jmsMsg = input.getValueByField("npos_message");
                posMsg = ((JMSMessage) jmsMsg);
                strMessage = convertStreamToString(posMsg);
                System.out.println("Recieved Message:"+ strMessage);

                @SuppressWarnings("rawtypes")
                Map parsedSegments = MessageParser.instance().parseMessage(strMessage);

                @SuppressWarnings("serial")
                Type nposMessageType = new TypeToken<Map<String, Map<String, String>>>() {
                }.getType();
                String segmentsJson = gson.toJson(parsedSegments, nposMessageType);

                System.out.println("Parsed JSON : "+segmentsJson);

                producer.send(new KeyedMessage<String, String>(topic, segmentsJson));
                this.collector.ack(input);
            } catch (Exception e) {
                producer = null;
                e.printStackTrace();
//                NPOSMessageDetail failedMessage = new NPOSMessageDetail(nposMessage.getTopologyID(), nposMessage.getNposMessage(),
//                        ExceptionUtils.getFullStackTrace(e), ComponentFailureEnum.KAFKA_BOLT, nposMessage.getRetryCount(), new Date());
//                this.collector.emit(new Values(failedMessage));
                this.collector.fail(input);
            }
        }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("npos_message"));
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
}