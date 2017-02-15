package com.booking.replication.configuration;

import com.booking.replication.metrics.MetricsReporter;

/**
 * Created by edmitriev on 2/15/17.
 */
public class MetricsReporterConfiguration {
    public String type;
    public String namespace;
    public String url;
    public MetricsReporter implementation;
}
