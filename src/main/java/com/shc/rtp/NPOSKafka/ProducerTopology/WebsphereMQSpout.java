package com.shc.rtp.NPOSKafka.ProducerTopology;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Session;

import com.shc.rtp.NPOSKafka.notification.model.NotificationModel;
import com.shc.rtp.common.NPOSConstant;
import com.shc.rtp.enums.ComponentFailureEnum;
import com.shc.rtp.enums.FieldEnum;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ibm.jms.JMSMessage;
import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueReceiver;
import com.ibm.mq.jms.MQQueueSession;

/**
 * Created by uchaudh on 11/17/2015.
 */
public class WebsphereMQSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private MQQueueReceiver receiver;
    private MQQueueSession queueSession;
    private MQQueueConnection queueConnection;

    private String hostName;
    private int port;
    private String queueManager;
    private String queueChannel;
    private String queueName;

    private boolean firstTimeactivateFlag = true;
    private long messageCount = 0;

    private static final String pattern = "MM/dd/yyyy HH:mm:ss.SSS";
    DateTime prevDateTime=new DateTime().minusHours(1);

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WebsphereMQSpout.class);

    /**
     * Constructor
     *
     * @param hostName
     *           - host name
     * @param port
     *           - port to connect to
     * @param queueManager
     *           - queue manager name
     * @param queueChannel
     *           - queue channel name
     * @param queueName
     *           - queue name
     */
    public WebsphereMQSpout(final String hostName, final int port, final String queueManager, final String queueChannel,
                            final String queueName) {
        this.hostName = hostName;
        this.port = port;
        this.queueManager = queueManager;
        this.queueChannel = queueChannel;
        this.queueName = queueName;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            initConnection();
        } catch (JMSException e) {
            LOG.error("Exception occurred while establishing queue connection", e);
            closeConnections(queueSession, receiver, queueConnection);
        }
    }

    @Override
    public void nextTuple() {
        try {
            JMSMessage receivedMessage = (JMSMessage) receiver.receive();
            ++messageCount;
            if(new DateTime().getHourOfDay() != prevDateTime.getHourOfDay())
            {
                System.out.println("No. of messages: "+messageCount+"\tAt Time: "+ new DateTime().toString(pattern));
                prevDateTime= new DateTime();
                messageCount=0;
            }
            collector.emit(NPOSConstant.STREAM_MQ_SPOUT_MSG_RECEIVE_SUCCESS_STREAM, new Values(receivedMessage), receivedMessage);
        } catch (JMSException e) {
            //just log the high level description of the message
            LOG.error("Exception occurred while receiving message from queue ", e.getMessage());
            // create notification model and send it to evaluator for evaluating
            String errorMessage = ExceptionUtils.getFullStackTrace(e);
            NotificationModel notificationModel = new NotificationModel(NPOSConstant.TOPOLOGY_NAME, errorMessage, new DateTime(),
                    ComponentFailureEnum.MQ_SPOUT);
            collector.emit(NPOSConstant.STREAM_MQ_SPOUT_MSG_RECEIVE_FAILURE_STREAM, new Values(notificationModel), notificationModel);

            // check for MQJMS2005 - connection error
            // TODO: add a check on list of error codes
            if("MQJMS2005".equalsIgnoreCase(e.getErrorCode())) {
                boolean isConnOpen = false;
                closeConnections(queueSession, receiver, queueConnection); //close all existing connection
                do {
                    //wait for 1 minute
                    try { Thread.sleep(10000); } catch (InterruptedException ignore) { }
                    try {
                        initConnection();
                        isConnOpen = true;
                    }
                    catch (JMSException e1) {
                        isConnOpen = false;
                    }
                } 	while(isConnOpen) ;
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        // do nothing for now
    }

    @Override
    public void fail(Object msgId) {
        // do nothing for now
    }

    @Override
    public void close() {
        closeConnections(queueSession, receiver, queueConnection);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(NPOSConstant.STREAM_MQ_SPOUT_MSG_RECEIVE_SUCCESS_STREAM, new Fields(FieldEnum.FIELD_NPOS_MESSAGE.getFieldName()));
        declarer.declareStream(NPOSConstant.STREAM_MQ_SPOUT_MSG_RECEIVE_FAILURE_STREAM, new Fields(FieldEnum.FIELD_NOTIFICATION_DETAILS.getFieldName()));
    }

    @Override
    public void activate(){
        if (!firstTimeactivateFlag){
            try{
                initConnection();
            }catch (JMSException e) {
                LOG.error("Exception occurred while establishing queue connection", e);
                closeConnections(queueSession, receiver, queueConnection);
            }
        }else{
            firstTimeactivateFlag = false;
        }
    }

    @Override
    public void deactivate(){
        closeConnections(queueSession, receiver, queueConnection);
    }

    private void initConnection() throws JMSException {
        MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
        cf.setHostName(hostName);
        cf.setPort(port);
        cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        cf.setQueueManager(queueManager);
        cf.setChannel(queueChannel);

        queueConnection = (MQQueueConnection) cf.createQueueConnection();
        queueSession = (MQQueueSession) queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        MQQueue queue = (MQQueue) queueSession.createQueue(queueName);
        receiver = (MQQueueReceiver) queueSession.createReceiver(queue);
        queueConnection.start();
    }

    /**
     * Close connections
     *
     * @param session
     *           - queue session
     * @param receiver
     *           - queue receiver
     * @param connection
     *           - queue connection
     */
    private static void closeConnections(final MQQueueSession session, final MQQueueReceiver receiver, final MQQueueConnection connection) {
        try { if(receiver!=null) { receiver.close(); } } catch (JMSException e) { }
        try { if(session!=null) { session.close(); } } catch (JMSException e) { }
        try { if(connection!=null) { connection.close(); } } catch (JMSException e) { }
    }
}