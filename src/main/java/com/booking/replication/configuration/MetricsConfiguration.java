package com.booking.replication.configuration;

import com.booking.replication.util.Duration;

import javax.naming.ConfigurationException;
import java.util.HashMap;

/**
 * Created by edmitriev on 2/15/17.
 */
public class MetricsConfiguration {
    private Duration frequency;
    private HashMap<String, MetricsReporterConfiguration> reporters;

    public MetricsConfiguration(Duration frequency, HashMap<String, MetricsReporterConfiguration> reporters) throws ConfigurationException {
        this.frequency = frequency;
        this.reporters = reporters;
    }

    public Duration getFrequency() {
        return frequency;
    }

    public HashMap<String, MetricsReporterConfiguration> getReporters() {
        return reporters;
    }

    /**
     * Get metrics reporter configuration.
     *
     * @param type The type of reporter
     * @return Configuration object
     */
    public MetricsReporterConfiguration getReporter(String type) {
        return reporters.getOrDefault(type, null);
    }

}