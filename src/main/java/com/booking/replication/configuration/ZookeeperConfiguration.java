package com.booking.replication.configuration;

import com.google.common.base.Joiner;

import javax.naming.ConfigurationException;
import java.util.List;

/**
 * Created by edmitriev on 2/15/17.
 */
public class ZookeeperConfiguration {
    public List<String> quorum;
    public String path;

    public ZookeeperConfiguration(List<String> quorum, String path) throws ConfigurationException {
        if (quorum == null) throw new ConfigurationException("Metadata store set as zookeeper but no zookeeper quorum is specified");
    }

    public List<String> getQuorum() {
        return quorum;
    }

    public String getQuorumString() {
        return Joiner.on(",").join(quorum);
    }

    public String getPath() {
        return path;
    }
}
