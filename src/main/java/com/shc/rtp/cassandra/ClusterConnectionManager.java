package com.shc.rtp.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.shc.rtp.common.NPOSConfiguration;

/**
 * Created by uchaudh on 10/16/2015.
 */
public class ClusterConnectionManager {


    private static Session session;
    private static Cluster cluster;

    public void connect(NPOSConfiguration config)
    {
        cluster = Cluster.builder()
                .addContactPoints(config.getString("cassandra.cluster.contactpoints"))
//                .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("US_EAST"))
                .build();
        cluster.getConfiguration()
                .getProtocolOptions();
//                .setCompression(ProtocolOptions.Compression.LZ4);

        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",metadata.getClusterName());
        session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }


}
