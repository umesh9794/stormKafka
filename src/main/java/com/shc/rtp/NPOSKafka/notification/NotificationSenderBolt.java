package com.shc.rtp.NPOSKafka.notification;

import java.util.Map;

import com.shc.rtp.NPOSKafka.notification.model.NotificationModel;
import com.shc.rtp.common.NPOSConfiguration;
import com.shc.rtp.common.NPOSConstant;
import com.shc.rtp.enums.FieldEnum;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.MultiPartEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by uchaudh on 10/15/2015.
 */
public class NotificationSenderBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;

    private MultiPartEmail email;

    private static final NPOSConfiguration CONFIGURATION = new NPOSConfiguration();
    private static final Logger LOG = LoggerFactory.getLogger(NotificationSenderBolt.class);

    private String emailToList;

    public NotificationSenderBolt(final String emailToList){
        this.emailToList = emailToList;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    private void addReceipients() throws EmailException{
        String [] toList = emailToList.trim().split(";");
        for (String emailID : toList) {
            email.addTo(emailID);
        }
    }

    private String getHtmlText(NotificationModel notification){
        StringBuilder sb = new StringBuilder();
        sb.append("<html>" +
                "<font face='Callibri' size=5>" +
                "Failed Component:"+notification.getComponentFailureEnum().toString()+"</font>" +
                "<br><br>" +
                "<table border=1 width=600 height=100 cellpadding=3 cellspacing=3>" +
                "<tr><td width=30%><b>Failed Component</b></td>" +
                "<td>"+notification.getComponentFailureEnum().toString()+"</td></tr>" +
                "<tr><td width=30%><b>Error Date and Time</b></td>" +
                "<td>"+notification.getErrorTS().toString()+"</td></tr>" +
                "<tr><td width=30%><b>Error Description</b></td>" +
                "<td>"+notification.getErrorDescription()+"</td></tr>" +
                "</table><br>" +
                "<p><font face='Callibri' size=3>" +
                "This is auto generated mail, please do not replay.</font><br>" +
                "<p>Thank-You<br>" +
                "RTP Engineering Team &nbsp" +
                "(RTP_Engineering_Team@searshc.com)");
        return sb.toString();
    }

    @Override
    public void execute(Tuple input) {
        NotificationModel notificationModel = (NotificationModel) input.getValueByField(FieldEnum.FIELD_NOTIFICATION_DETAILS.getFieldName());
        try {
            email = new MultiPartEmail();
            email.setHostName(CONFIGURATION.getString(NPOSConstant.PROP_MAIL_SERVER_HOST));
            email.setSSLOnConnect(false);
            email.setFrom(CONFIGURATION.getString(NPOSConstant.PROP_MAIL_FROM));
            email.setSubject("NPOS (QA) System Notification: DO NOT REPLY");
            email.addPart(getHtmlText(notificationModel), "text/html");
            addReceipients();
            email.send();
        } catch (EmailException e) {
            LOG.error("Exception occurred while sending notification email", e);
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
