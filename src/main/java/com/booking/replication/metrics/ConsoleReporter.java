package com.booking.replication.metrics;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.util.Duration;

import com.codahale.metrics.ScheduledReporter;

import java.util.concurrent.TimeUnit;

/**
 * This class provides a Console Reporter.
 */
public class ConsoleReporter extends MetricsReporter {

    /**
     * Start a metrics console reporter.
     */
    public ConsoleReporter(Duration frequency) {
        this.frequency = frequency;

        reporter = com.codahale.metrics.ConsoleReporter.forRegistry(Metrics.registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
    }

}
