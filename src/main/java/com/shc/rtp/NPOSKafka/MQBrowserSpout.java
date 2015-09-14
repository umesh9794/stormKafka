package com.shc.rtp.NPOSKafka;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.util.Enumeration;
import java.util.Map;
import java.util.Random;

import javax.jms.JMSException;
import javax.jms.Session;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.ibm.jms.JMSMessage;
import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueBrowser;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueSession;

/**
 * Created by uchaudh on 9/10/2015.
 */
public class MQBrowserSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1723792244453157513L;
    private SpoutOutputCollector _collector;
    private MQQueueBrowser browser;
    private MQQueueSession queueSession;
    private MQQueueConnection queueConnection;
    @SuppressWarnings("rawtypes")
    private Enumeration enumeration;
    private long messageCount = 0;
    private String queueName;
    private Random random = new Random();

    public MQBrowserSpout(String queueName) {
        this.queueName = queueName;
    }

    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            MQQueueConnectionFactory cf = createMQQueueConnectionFactory();
            queueConnection = (MQQueueConnection) cf.createQueueConnection();
            queueSession = (MQQueueSession) queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            MQQueue queue = (MQQueue) queueSession.createQueue(queueName);
            browser = (MQQueueBrowser) queueSession.createBrowser(queue);
            queueConnection.start();
            enumeration = browser.getEnumeration();

        } catch (JMSException e) {
            try {
                throw e;
            } catch (JMSException e1) {
                e1.printStackTrace();
            }
        }
    }

    private MQQueueConnectionFactory createMQQueueConnectionFactory() throws JMSException {
        MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
//		cf.setHostName("hofmdsappstrs1.sears.com");
//		cf.setPort(1414);
//		cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
//		cf.setQueueManager("SQAT0263");
//		cf.setChannel("STORM.SVRCONN");

        cf.setHostName("hfqamqsvr3.vm.itg.corp.us.shldcorp.com");
        cf.setPort(1414);
        cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        cf.setQueueManager("SQCT0001");
        cf.setChannel("STORM.SVRCONN");
        return cf;
    }

    public void nextTuple() {
        JMSMessage receivedMessage = null;
        if (enumeration.hasMoreElements()) {
//			try {
//				Thread.sleep(random.nextInt(100));
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}

            //			if (messageCount <= 100) {
            receivedMessage = (JMSMessage) enumeration.nextElement();
            ++messageCount;
            System.out.println("Message counter " +messageCount);
            _collector.emit("mq_spout_msg_receive_success_stream", new Values(receivedMessage), receivedMessage);

            //			}
        }
    }

    @Override
    public void close() {
        try {
            queueSession.close();
            browser.close();
            queueConnection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("mq_spout_msg_receive_success_stream", new Fields("npos_message"));
        //		declarer.declareStream(NPOSConstant.STREAM_MQ_SPOUT_MSG_RECEIVE_FAILURE_STREAM, new Fields(FieldEnum.FIELD_NOTIFICATION_DETAILS.getFieldName()));
    }



}
