package com.shc.rtp.NPOSKafka.ConsumerTopology;


import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Session;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ibm.jms.JMSTextMessage;
import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueSender;
import com.ibm.mq.jms.MQQueueSession;

/**
 * Created by uchaudh on 10/1/2015.
 */
public class MQSenderBolt extends BaseRichBolt {

    private static final long serialVersionUID = -128031238218711051L;

    private MQQueueSender sender;
    private MQQueueSession senderQueueSession;
    private MQQueueConnection senderQueueConnection;
    private OutputCollector collector;

    private String hostName;
    private int port;
    private String queueManager;
    private String queueChannel;
    private String queueName;
    private boolean isClusterQueue;

//    private ComponentFailureEnum componentFailureEnum;

    public MQSenderBolt(final String hostName, final int port, final String queueManager, final String queueChannel, final String queueName) {
        this.hostName = hostName;
        this.port = port;
        this.queueManager = queueManager;
        this.queueChannel = queueChannel;
        this.queueName = queueName;
//        this.isClusterQueue = isClusterQueue;
//        this.componentFailureEnum = componentFailureEnum;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            openConnection();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("npos_message"));
    }

    @Override
    public void execute(Tuple input) {

            try {
                JMSTextMessage textMsg = (JMSTextMessage) senderQueueSession.createTextMessage(input.toString());
                sender.send(textMsg);
                this.collector.ack(input);
            } catch (Exception e) {
                sender = null;
                closeConnections(senderQueueSession, sender, senderQueueConnection);
//                collector.emit(new Values(failedMessage));
//                collector.fail(input);
            }

    }

    @Override
    public void cleanup() {
        closeConnections(senderQueueSession, sender, senderQueueConnection);
    }

    private void openConnection() throws JMSException{
        MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
        cf.setHostName(this.hostName);
        cf.setPort(this.port);
        cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        // do not set queue manager for cluster queue
        if (this.isClusterQueue) {
            cf.setQueueManager(this.queueManager);
        }
        cf.setChannel(this.queueChannel);
        senderQueueConnection = (MQQueueConnection) cf.createQueueConnection();
        senderQueueSession = (MQQueueSession) senderQueueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        sender = (MQQueueSender) senderQueueSession.createSender(senderQueueSession.createQueue(this.queueName));
        senderQueueConnection.start();
    }

    private static void closeConnections(final MQQueueSession session, final MQQueueSender sender, final MQQueueConnection connection) {
        try {
            if (sender != null) {
                sender.close();
            }
        } catch (JMSException e) {
        }
        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException e) {
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
        }
    }
}
