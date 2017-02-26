package com.booking.replication.applier.kafka;

import com.booking.replication.applier.*;
import com.booking.replication.configuration.*;
import com.booking.replication.validation.ValidationService;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Meter;

import java.io.IOException;


/**
 * Created by edmitriev on 2/24/17.
 */
public class ApplierFactory {
    public static Applier getApplier() {
        return new StdoutJsonApplier();
    }

    public static Applier getApplier(MainConfiguration mainConfiguration, HBaseConfiguration hBaseConfiguration,
                                     ReplicationSchemaConfiguration replicationSchemaConfiguration,
                                     ValidationConfiguration validationConfiguration, Counting mainProgressCounter,
                                     ValidationService validationService
    ) {
        return new HBaseApplier(mainConfiguration, hBaseConfiguration, replicationSchemaConfiguration, validationConfiguration,
                (Counter) mainProgressCounter, validationService);
    }

    public static Applier getApplier(KafkaConfiguration kafkaConfiguration, Counting mainProgressCounter) throws IOException {
        return new KafkaApplier(kafkaConfiguration, (Meter) mainProgressCounter);
    }
}
