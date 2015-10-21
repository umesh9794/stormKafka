package com.shc.rtp.enums;

/**
 * Enums for the columns in column family
 * Created by uchaudh on 10/15/2015.
 */
public enum FieldEnum {
    FIELD_ROW_KEY("key"),
    FIELD_TOPOLOGY_ID("topology_id"),
    FIELD_NPOS_MESSAGE("npos_message"),
    FIELD_ERROR_MESSAGE("error_message"),
    FIELD_RETRY_COUNT("retry_count"),
    FIELD_NOTIFICATION_DETAILS("notification_details"),
    FIELD_ERROR_DATE("error_date"),
    FIELD_EVENT_TIME("event_time"),
    FIELD_REPLAY_FLAG("replay_required_flag");

    private String fieldName;

    private FieldEnum(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return this.fieldName;
    }
}