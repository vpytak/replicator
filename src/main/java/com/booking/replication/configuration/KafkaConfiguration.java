package com.booking.replication.configuration;

import java.util.List;

/**
 * Created by edmitriev on 2/15/17.
 */
public class KafkaConfiguration {
    boolean isDryRunMode;
    private String broker;
    private List<String> tables;
    private List<String> excludeTables;
    private String topic;

    public KafkaConfiguration(boolean isDryRunMode, String broker, List<String> tables, List<String> excludeTables, String topic) {
        this.isDryRunMode = isDryRunMode;
        this.broker = broker;
        this.tables = tables;
        this.excludeTables = excludeTables;
        this.topic = topic;
    }

    public String getBroker() {
        return broker;
    }

    public List<String> getTables() {
        return tables;
    }

    public List<String> getExcludeTables() {
        return excludeTables;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isDryRunMode() {
        return isDryRunMode;
    }

}
