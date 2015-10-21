package com.shc.rtp.NPOSKafka.notification.model;

import com.shc.rtp.enums.ComponentFailureEnum;
import org.joda.time.DateTime;

/**
 * Created by uchaudh on 10/15/2015.
 */
public class NotificationModel implements ModelIF {

    private static final long serialVersionUID = -1470013350402817092L;
    private String topologyID;
    private String errorDescription;
    private DateTime errorTS;
    private ComponentFailureEnum componentFailureEnum;

    /**
     * Constructor
     *
     * @param topologyID
     * @param errorDescription
     * @param errorTS
     * @param componentFailureEnum
     */
    public NotificationModel(String topologyID, String errorDescription, DateTime errorTS, ComponentFailureEnum componentFailureEnum) {
        this.topologyID = topologyID;
        this.errorDescription = errorDescription;
        this.errorTS = errorTS;
        this.componentFailureEnum = componentFailureEnum;

    }

    /**
     * @return
     */
    public String getTopologyID() {
        return this.topologyID;
    }

    /**
     * @return
     */
    public String getErrorDescription() {
        return this.errorDescription;
    }

    /**
     * @return
     */
    public DateTime getErrorTS() {
        return errorTS;
    }

    /**
     * @return
     */
    public ComponentFailureEnum getComponentFailureEnum() {
        return componentFailureEnum;
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(" Error Description Is: " + this.errorDescription);
        strBuilder.append(" Error Time Stamp Is: " + this.errorTS);
        strBuilder.append(" Topology Id Is : " + this.topologyID);
        strBuilder.append(" Component Failure Is: " + this.componentFailureEnum.toString());
        return strBuilder.toString();
    }
}
