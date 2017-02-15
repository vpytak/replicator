package com.booking.replication.configuration;

/**
 * Created by edmitriev on 2/15/17.
 */
public class ValidationConfiguration {
    private String broker;
    private String topic;
    private String tag;
    private String sourceDomain;
    private String targetDomain;
    private long throttling;

    public ValidationConfiguration(String broker, String topic, String tag, String sourceDomain, String targetDomain, long throttling) {
        this.broker = broker;
        this.topic = topic;
        this.tag = tag;
        this.sourceDomain = sourceDomain;
        this.targetDomain = targetDomain;
        this.throttling = throttling;
    }

    public String getBroker() {
        return broker;
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

    public String getSourceDomain() {
        return sourceDomain;
    }

    public String getTargetDomain() {
        return targetDomain;
    }

    public long getThrottling() { return throttling; }

}
