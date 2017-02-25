package com.booking.replication.metrics;

import com.booking.replication.Metrics;
import com.codahale.metrics.Counting;

import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 2/25/17.
 */
public class MainProgressCounter {
    private Counting counter;
    private String description;
    private static Map<String, MainProgressCounter> instances = new HashMap<>();

    public static MainProgressCounter getInstance(String type) {
        MainProgressCounter instance = instances.get(type);
        if (instance == null) {
            instance = new MainProgressCounter(type);
            instances.put(type, instance);
        }
        return instance;
    }

    private MainProgressCounter(String type) {
        switch (type) {
            case "hbase":
                counter = Metrics.registry.counter(name("HBase", "applierTasksSucceededCounter"));
                description = "# of HBase tasks that have succeeded";
                break;
            case "kafka":
                counter = Metrics.registry.meter(name("Kafka", "producerToBroker"));
                description = "# of messages pushed to the Kafka broker";
                break;
            default:
                throw new RuntimeException(String.format("Unknown counter type: %s", type));
        }
    }

    public Counting getCounter() {
        return counter;
    }

    public String getDescription() {
        return description;
    }
}
