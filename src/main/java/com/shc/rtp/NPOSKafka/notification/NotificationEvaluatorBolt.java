package com.shc.rtp.NPOSKafka.notification;

import java.util.Map;

import com.shc.rtp.NPOSKafka.notification.model.NotificationModel;
import com.shc.rtp.common.NPOSConfiguration;
import com.shc.rtp.common.NPOSConstant;
import com.shc.rtp.enums.FieldEnum;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * Created by uchaudh on 10/15/2015.
 */
public class NotificationEvaluatorBolt extends BaseRichBolt {

    private static final long serialVersionUID = -4261488068349296727L;
    private static final NPOSConfiguration CONFIGURATION = new NPOSConfiguration();
    private int notificationWindow;
    private OutputCollector collector;
    private static DateTime lastErrorTS = null;
    private static final Logger LOG = LoggerFactory.getLogger(NotificationEvaluatorBolt.class);

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.notificationWindow = CONFIGURATION.getInt(NPOSConstant.PROP_NOTIFICATION_WINDOW);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FieldEnum.FIELD_NOTIFICATION_DETAILS.getFieldName()));
    }

    @Override
    /**
     * method will get called for each failure message emitted from mq bolt.
     */
    public void execute(Tuple input) {
        try {
            NotificationModel notificationModel = (NotificationModel) input.getValueByField(FieldEnum.FIELD_NOTIFICATION_DETAILS
                    .getFieldName());
            // if date time is null ( meaning there was no failure prior to this )
            // initialize it with current tS
            if (lastErrorTS == null) {
                lastErrorTS = new DateTime();
                collector.emit(new Values(notificationModel));
                LOG.info("*** Sending message for the notification for the 1st failure in permitted time window");
            }
            // calculate difference between two dates in minutes
            // if window is elapsed send notification
            else {
                // Taking absolute value to avoid negative value in case lasTS > currentTS
                // (case when lastTS in initialized with current value because this is the 1st error message
                int absMinDiff = Math.abs(Minutes.minutesBetween(notificationModel.getErrorTS(), lastErrorTS).getMinutes());
                if (absMinDiff >= notificationWindow) {
                    // initialize lastTS with currentTS
                    lastErrorTS = notificationModel.getErrorTS();
                    collector.emit(new Values(notificationModel));
                    LOG.info("*** Sending message for the notification for notification window elapsed");
                } else {
                    LOG.info("### NOT - Sending message for the notification because notification window has not elapsed");
                }
            }
        } finally {
            collector.ack(input);
        }
    }
}
