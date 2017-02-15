package com.booking.replication;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.configuration.MetricsConfiguration;
import com.booking.replication.metrics.GraphiteReporter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

/**
 * This class provides facilities for using the Dropwizard-MetricsConfiguration library.
 */
public class Metrics {
    public static MetricRegistry registry = new MetricRegistry();

    public static void setRegistry(MetricRegistry reg) {
        registry = reg;
    }

    /**
     * Start metric reporters.
     */
    public static void startReporters(MetricsConfiguration metricsConfiguration) {
        registry.register(name("jvm", "gc"), new GarbageCollectorMetricSet());
        registry.register(name("jvm", "threads"), new ThreadStatesGaugeSet());
        registry.register(name("jvm", "classes"), new ClassLoadingGaugeSet());
        registry.register(name("jvm", "fd"), new FileDescriptorRatioGauge());
        registry.register(name("jvm", "memory"), new MemoryUsageGaugeSet());

        metricsConfiguration.reporters.values().forEach( (v) -> v.implementation.start() );
    }

}


