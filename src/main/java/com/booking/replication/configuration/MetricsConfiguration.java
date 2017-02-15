package com.booking.replication.configuration;

import com.booking.replication.Configuration;
import com.booking.replication.metrics.GraphiteReporter;
import com.booking.replication.util.Duration;

import javax.naming.ConfigurationException;
import java.util.HashMap;

/**
 * Created by edmitriev on 2/15/17.
 */
public class MetricsConfiguration {


    public Duration frequency;
    public HashMap<String, MetricsReporterConfiguration> reporters;

    public MetricsConfiguration(Duration frequency, HashMap<String, MetricsReporterConfiguration> reporters) throws ConfigurationException {
        this.frequency = frequency;
        this.reporters = reporters;
        for (String reporterName : reporters.keySet()) {
            MetricsReporterConfiguration reporter = reporters.get(reporterName);
            switch (reporterName) {
                case "graphite":
                    reporter.implementation = new GraphiteReporter(frequency, reporter);
                    break;
                case "console":
                    reporter.implementation = new com.booking.replication.metrics.ConsoleReporter(frequency);
                    break;
                default:
                    throw new ConfigurationException("No implementation found for reporter type: " + reporterName);
            }
        }
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