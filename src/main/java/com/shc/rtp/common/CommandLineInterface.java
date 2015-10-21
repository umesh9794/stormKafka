package com.shc.rtp.common;


import java.util.HashMap;

/**
 * Created by uchaudh on 10/15/2015.
 */
public class CommandLineInterface {
    private HashMap<String,String> topologyOptionMap = new HashMap<String,String>();

    public CommandLineInterface(){
        topologyOptionMap.put(NPOSConstant.TOPOLOGY_NAME,"");
        topologyOptionMap.put(NPOSConstant.CONNECTION_REFRESH_INTERVAL,"5");
        topologyOptionMap.put(NPOSConstant.WAIT_TIME,"5");
        topologyOptionMap.put(NPOSConstant.NUMBER_OF_ACKERS,"20");
        topologyOptionMap.put(NPOSConstant.NUMBER_OF_WORKERS,"30");
        topologyOptionMap.put(NPOSConstant.MESSAGE_TIMEOUT_SECONDS,"300");
        topologyOptionMap.put(NPOSConstant.PARSER_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.STREAM_CREATOR_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.SHC_FLUME_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.SHO_KAFKA_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.MA_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.RFID_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.PRICING_FEED_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.MYSQL_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.CASSANDRA_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.REPLAY_HANDLER_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.LOGGER_BOLT_PARALLEL,"10");
        topologyOptionMap.put(NPOSConstant.NOTIFICATION_EVALUATOR_BOLT_PARALLEL,"5");
        topologyOptionMap.put(NPOSConstant.NOTIFICATION_SENDER_BOLT_PARALLEL,"1");
        topologyOptionMap.put(NPOSConstant.EMAIL_TO_LIST,"RTP_Engineering_Team@searshc.com");
    }

    public CommandLineInterface(String [] argumentArray){
        this();
        parseArguments(argumentArray);
    }

    private void parseArguments(String[] argumentArray) {
        for (String argument : argumentArray) {
            String parameters[] = argument.split(NPOSConstant.CLI_DELIMITER, NPOSConstant.CLI_SPLIT_LIMIT);
            String option = parameters[0].substring(1);
            String value = parameters[1];
            topologyOptionMap.put(option, value);
        }
    }

    public int getInteger(String key){
        return Integer.parseInt(topologyOptionMap.get(key));
    }

    public String getString(String key){
        return topologyOptionMap.get(key);
    }
}

