package com.shc.rtp.enums;

/**
 * Created by uchaudh on 10/15/2015.
 */
public enum ComponentFailureEnum {

    PARSER_BOLT(ColumnFamilyEnum.PARSER_FAILED_FAM.getColumnFamilyName(), "parser_bolt_retry_stream"),
    CASSANDRA_BOLT(ColumnFamilyEnum.CASSANDRADB_FAILED_FAM.getColumnFamilyName(), "cassandra_bolt_retry_stream"),
    SHO_FLUME_BOLT(ColumnFamilyEnum.SHO_FLUME_FAILED_FAM.getColumnFamilyName(), "sho_flume_bolt_retry_stream"),
    SHC_FLUME_BOLT(ColumnFamilyEnum.SHC_FLUME_FAILED_FAM.getColumnFamilyName(), "shc_flume_bolt_retry_stream"),
    MQ_SPOUT(ColumnFamilyEnum.NO_FAM.getColumnFamilyName(), "no_retry_stream"),
    KAFKA_BOLT(ColumnFamilyEnum.KAFKA_FAILED_FAM.getColumnFamilyName(),"kafka_bolt_retry_stream"),
    PRICING_FEED_BOLT(ColumnFamilyEnum.PRICING_FEED_FAILED_FAM.getColumnFamilyName(),"pricing_feed_bolt_retry_stream"),
    MYSQL_BOLT(ColumnFamilyEnum.MYSQL_FAILED_FAM.getColumnFamilyName(), "mysql_bolt_retry_stream"),
    MA_BOLT(ColumnFamilyEnum.MA_FEED_FAILED_FAM.getColumnFamilyName(), "ma_bolt_retry_stream"),
    RFID_BOLT(ColumnFamilyEnum.RFID_FEED_FAILED_FAM.getColumnFamilyName(), "rfid_bolt_retry_stream");

    private String columnFamilyName;
    private String retryStreamName;

    private ComponentFailureEnum(String columnFamilyName, String retryStreamName) {
        this.columnFamilyName = columnFamilyName;
        this.retryStreamName = retryStreamName;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public String getRetryStreamName() {
        return retryStreamName;
    }

    public static ComponentFailureEnum fromCFName(String cfName) {
        ComponentFailureEnum componentFailureEnum = null;
        if (ColumnFamilyEnum.CASSANDRADB_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.CASSANDRA_BOLT;
        } else if (ColumnFamilyEnum.SHO_FLUME_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.SHO_FLUME_BOLT;
        } else if (ColumnFamilyEnum.SHC_FLUME_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.SHC_FLUME_BOLT;
        } else if (ColumnFamilyEnum.PARSER_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.PARSER_BOLT;
        } else if (ColumnFamilyEnum.KAFKA_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.KAFKA_BOLT;
        }  else if (ColumnFamilyEnum.MA_FEED_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.MA_BOLT;
        } else if (ColumnFamilyEnum.RFID_FEED_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.RFID_BOLT;
        } else if (ColumnFamilyEnum.PRICING_FEED_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.PRICING_FEED_BOLT;
        } else if (ColumnFamilyEnum.MYSQL_FAILED_FAM.getColumnFamilyName().equalsIgnoreCase(cfName)) {
            componentFailureEnum = ComponentFailureEnum.MYSQL_BOLT;
        }
        return componentFailureEnum;
    }
}

enum ColumnFamilyEnum {
    PARSER_FAILED_FAM("parser_failed_fam"),
    CASSANDRADB_FAILED_FAM("cassandra_failed_fam"),
    SHO_FLUME_FAILED_FAM("sho_flume_failed_fam"),
    SHC_FLUME_FAILED_FAM("shc_flume_failed_fam"),
    KAFKA_FAILED_FAM("kafka_failed_fam"),
    PRICING_FEED_FAILED_FAM("pricing_feed_failed_fam"),
    MYSQL_FAILED_FAM("mysql_failed_fam"),
    MA_FEED_FAILED_FAM("ma_feed_failed_fam"),
    RFID_FEED_FAILED_FAM("rfid_feed_failed_fam"),
    NO_FAM("no_fam");

    private String columnFamilyName;

    private ColumnFamilyEnum(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public String getColumnFamilyName() {
        return this.columnFamilyName;
    }
}