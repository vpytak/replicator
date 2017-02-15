package com.booking.replication.metrics;

import com.booking.replication.util.Duration;

import com.codahale.metrics.ScheduledReporter;

/**
 * Created by rmirica on 06/06/16.
 */
public abstract class MetricsReporter {
    Duration frequency;
    ScheduledReporter reporter;

    private Duration getFrequency() {
        return frequency;
    }

    private ScheduledReporter getReporter() {
        return reporter;
    }

    /**
     * Start the reporter.
     */
    public void start() {
        getReporter().start(
                getFrequency().getQuantity(),
                getFrequency().getUnit()
        );
    }

}
