package com.shc.rtp.common;

/**
 * Constant Class
 * Created by uchaudh on 10/15/2015.
 */
public class NPOSConstant {

    private NPOSConstant() {
    }

    public static final String EMPTY_STRING = "";
    public static final String SPACE = " ";

    public static final String ENCODING_UTF8 = "UTF-8";

    public static final String SIGNAL_FIELD = "signal";
    public static final String SIGNAL_VALUE = "check_connections";

    public static final String PROP_CASSANDRA_CLUSTER_NAME = "cassandra.cluster.name";
    public static final String PROP_CASSANDRA_CLUSTER_KEYSPACE = "cassandra.cluster.keyspace";
    public static final String PROP_CASSANDRA_CONNECTION_POOL_NAME = "cassandra.connection.pool.name";
    public static final String PROP_CASSANDRA_SERVER_PORT = "cassandra.server.port";
    public static final String PROP_CASSANDRA_MAX_CONNECTIONS_PER_HOST = "cassandra.max.connections.per.host";
    public static final String PROP_CASSANDRA_CONNECTION_SEEDS = "cassandra.connection.seeds";
    public static final String PROP_CASSANDRA_COLUMN_FAMILY = "cassandra.column.family";
    public static final String PROP_CASSADNRA_USER_NAME="cassandra.user.name";
    public static final String PROP_CASSANDRA_USER_PASSWORD = "cassadnra.user.password";

    public static final String PROP_SHO_FLUME_HOSTS = "sho.flume.hosts";
    public static final String PROP_SHO_FLUME_CLIENT_TYPE = "sho.flume.client.type";
    public static final String PROP_SHO_FLUME_HOST_SELECTOR = "sho.flume.host.selector";
    public static final String PROP_SHC_FLUME_HOSTS = "shc.flume.hosts";
    public static final String PROP_SHC_FLUME_CLIENT_TYPE = "shc.flume.client.type";
    public static final String PROP_SHC_FLUME_HOST_SELECTOR = "shc.flume.host.selector";

    public static final String PROP_MAIN_QUEUE_HOST_NAME_1 = "main.queue.host.name_1";
    public static final String PROP_MAIN_QUEUE_HOST_PORT_1 = "main.queue.host.port_1";
    public static final String PROP_MAIN_QUEUE_QUEUE_MANAGER_1 = "main.queue.queue.manager_1";
    public static final String PROP_MAIN_QUEUE_CHANNEL_1 = "main.queue.channel_1";
    public static final String PROP_MAIN_QUEUE_NAME_1 = "main.queue.queue.name_1";

    public static final String PROP_MAIN_QUEUE_HOST_NAME_2 = "main.queue.host.name_2";
    public static final String PROP_MAIN_QUEUE_HOST_PORT_2 = "main.queue.host.port_2";
    public static final String PROP_MAIN_QUEUE_QUEUE_MANAGER_2 = "main.queue.queue.manager_2";
    public static final String PROP_MAIN_QUEUE_CHANNEL_2 = "main.queue.channel_2";
    public static final String PROP_MAIN_QUEUE_NAME_2 = "main.queue.queue.name_2";

    public static final String PROP_MA_QUEUE_HOST_NAME = "ma.queue.host.name";
    public static final String PROP_MA_QUEUE_HOST_PORT = "ma.queue.host.port";
    public static final String PROP_MA_QUEUE_QUEUE_MANAGER = "ma.queue.queue.manager";
    public static final String PROP_MA_QUEUE_CHANNEL = "ma.queue.channel";
    public static final String PROP_MA_QUEUE_NAME = "ma.queue.queue.name";

    public static final String PROP_RFID_QUEUE_HOST_NAME = "rfid.queue.host.name";
    public static final String PROP_RFID_QUEUE_HOST_PORT = "rfid.queue.host.port";
    public static final String PROP_RFID_QUEUE_QUEUE_MANAGER = "rfid.queue.queue.manager";
    public static final String PROP_RFID_QUEUE_CHANNEL = "rfid.queue.channel";
    public static final String PROP_RFID_QUEUE_NAME = "rfid.queue.queue.name";

    public static final String PROP_RETRY_QUEUE_HOST_NAME = "retry.queue.host.name";
    public static final String PROP_RETRY_QUEUE_HOST_PORT = "retry.queue.host.port";
    public static final String PROP_RETRY_QUEUE_QUEUE_MANAGER = "retry.queue.queue.manager";
    public static final String PROP_RETRY_QUEUE_CHANNEL = "retry.queue.channel";
    public static final String PROP_RETRY_QUEUE_NAME = "retry.queue.queue.name";
    public static final String PROP_MAX_RETRY_COUNT = "max.retry.count";

    public static final String PROP_STORE_CACHE_REFRESH_INTERVAL = "store.cache.refresh.interval";

    public static final String PROP_DEBUG_REQUIRED = "debug.required";

    public static final String PROP_NOTIFICATION_WINDOW = "notification.window";
    public static final String PROP_MAIL_SERVER_HOST = "mail.server.host";
    public static final String PROP_MAIL_FROM = "mail.from";
    public static final String PROP_MAIL_TO = "mail.to";

    // pf.mysql.password
    public static final String PROP_PRICING_FEED_MYSQL_CONNECTION_URL = "pf.mysql.connection.url";
    public static final String PROP_PRICING_FEED_MYSQL_USERNAME = "pf.mysql.username";
    public static final String PROP_PRICING_FEED_MYSQL_PASSWORD = "pf.mysql.password";

    // Component Names Constants
    public static final String MQ_SPOUT_1 = "mq_spout_1";
    public static final String MQ_SPOUT_2 = "mq_spout_2";
    public static final String SIGNAL_SPOUT = "signal_spout";
    public static final String MSG_PARSER_BOLT = "msg_parser_bolt";
    public static final String STREAM_CREATOR_BOLT = "stream_creator_bolt";
    public static final String SHO_KAFKA_BOLT = "sho_kafka_bolt";
    public static final String SHC_FLUME_BOLT = "shc_flume_bolt";
    public static final String CASSANDRA_BOLT = "cassandra_bolt";
    public static final String PRICING_FEED_BOLT = "pricing_feed_bolt";
    public static final String PRICING_MYSQL_BOLT = "pricing_mysql_bolt";
    public static final String MA_BOLT = "ma_bolt";
    public static final String RFID_BOLT = "rfid_bolt";
    public static final String LOGGER_BOLT = "logger_bolt";
    public static final String NOTIFICATION_EVALUATOR_BOLT = "notification_evaluator_bolt";
    public static final String NOTIFICATION_SENDER_BOLT = "notification_sender_bolt";
    public static final String MQ_RETRY_SPOUT = "mq_retry_spout";
    public static final String RETRY_HANDLER_BOLT = "retry_handler_bolt";
    public static final String REPLAY_HANDLER_BOLT = "replay_handler_bolt";

    // Stream Names Constants
    public static final String STREAM_SHO_STREAM = "sho_stream";
    public static final String STREAM_SHC_STREAM = "shc_stream";

    public static final String STREAM_LOGGER_STREAM = "logger_stream";
    public static final String STREAM_NOTIFICATION_EVALUATOR_STREAM = "notification_evaluator_stream";

//	public static final String STREAM_KAFKA_FAILED_STREAM = "kafka_failed_stream";
//	public static final String STREAM_FLUME_FAILED_STREAM = "flume_failed_stream";
//	public static final String STREAM_RFID_FAILED_STREAM = "rfid_failed_stream";
//	public static final String STREAM_MA_FAILED_STREAM = "ma_failed_stream";
//	public static final String STREAM_MYSQL_FAILED_STREAM = "mysql_failed_stream";

    public static final String STREAM_PRICING_FEED_SUCCESS_STREAM = "pricing_feed_success_stream";
    public static final String STREAM_PRICING_FEED_FAILURE_STREAM = "pricing_feed_failure_stream";

    public static final String STREAM_MQ_SPOUT_MSG_RECEIVE_SUCCESS_STREAM = "mq_spout_msg_receive_success_stream";
    public static final String STREAM_MQ_SPOUT_MSG_RECEIVE_FAILURE_STREAM = "mq_spout_msg_receive_failure_stream";

    public static final String STREAM_MSG_PARSER_SUCCESS = "msg_parser_success";
    public static final String STREAM_MSG_PARSER_FAILURE = "msg_parser_failure";

    // Stream Names Constants for Replay
    public static final String STREAM_REPLAY_PARSER_STREAM = "replay_parser_stream";
    public static final String STREAM_REPLAY_CASSANDRA_STREAM = "replay_cassandra_stream";
    public static final String STREAM_REPLAY_SHC_FLUME_STREAM = "replay_shc_flume_stream";
    public static final String STREAM_REPLAY_SHO_KAFKA_STREAM = "replay_sho_kafka_stream";
    public static final String STREAM_REPLAY_MA_STREAM = "replay_ma_stream";
    public static final String STREAM_REPLAY_RFID_STREAM = "replay_rfid_stream";
    public static final String STREAM_REPLAY_PRICING_FEED_STREAM = "replay_pricing_feed_stream";
    public static final String STREAM_REPLAY_MYSQL_STREAM = "replay_mysql_stream";


    public static final String FLUME_RPC_HEADER_MESSAGE_ID = "MessageID";
    public static final String FLUME_RPC_HEADER_MESSAGE_ID_VALUE = "NPOSMessage";
    public static final String FLUME_PROP_CLIENT_TYPE = "client.type";
    public static final String FLUME_PROP_HOSTS = "hosts";
    public static final String FLUME_PROP_HOST_SELECTOR = "host-selector";
    public static final String FLUME_PROP_BACKOFF = "backoff";
    public static final String FLUME_PROP_MAX_BACKOFF = "maxBackoff";

    public static final String CLI_DELIMITER = "=";
    public static final int CLI_SPLIT_LIMIT = 2;

    public static final String TOPOLOGY_NAME = "topology-name";
    public static final String CONNECTION_REFRESH_INTERVAL = "connection-refresh-interval";
    public static final String WAIT_TIME= "wait-time";
    public static final String NUMBER_OF_ACKERS = "number-of-ackers";
    public static final String NUMBER_OF_WORKERS = "number-of-workers";
    public static final String MESSAGE_TIMEOUT_SECONDS = "message-timeout-seconds";
    public static final String PARSER_BOLT_PARALLEL = "parser-bolt-parallel";
    public static final String STREAM_CREATOR_BOLT_PARALLEL = "stream-creator-bolt-parallel";
    public static final String SHC_FLUME_BOLT_PARALLEL = "shc-flume-bolt-parallel";
    public static final String SHO_KAFKA_BOLT_PARALLEL = "sho-kafka-bolt-parallel";
    public static final String MA_BOLT_PARALLEL = "ma-store-bolt-parallel";
    public static final String RFID_BOLT_PARALLEL = "rfid-store-bolt-parallel";
    public static final String PRICING_FEED_BOLT_PARALLEL = "pricing-feed-bolt-parallel";
    public static final String MYSQL_BOLT_PARALLEL = "mysql-bolt-parallel";
    public static final String CASSANDRA_BOLT_PARALLEL = "cassandra-bolt-parallel";
    public static final String REPLAY_HANDLER_BOLT_PARALLEL = "replay-handler-bolt-parallel";
    public static final String LOGGER_BOLT_PARALLEL = "logger-bolt-parallel";
    public static final String NOTIFICATION_EVALUATOR_BOLT_PARALLEL = "notification-evaluator-bolt-parallel";
    public static final String NOTIFICATION_SENDER_BOLT_PARALLEL = "notofication-sender-bolt-parallel";
    public static final String EMAIL_TO_LIST = "email-to-list";

}

