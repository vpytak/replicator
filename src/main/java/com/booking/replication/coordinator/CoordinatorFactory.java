package com.booking.replication.coordinator;

import com.booking.replication.configuration.MetadataStoreConfiguration;

/**
 * Created by edmitriev on 2/15/17.
 */
public class CoordinatorFactory {

    public static CoordinatorInterface getCoordinator(MetadataStoreConfiguration metadataStoreConfiguration) {
        switch (metadataStoreConfiguration.getMetadataStoreType()) {
            case MetadataStoreConfiguration.METADATASTORE_ZOOKEEPER:
                return new ZookeeperCoordinator(metadataStoreConfiguration.getZookeeper());
            case MetadataStoreConfiguration.METADATASTORE_FILE:
                return new FileCoordinator(metadataStoreConfiguration.getFile());
            default:
                throw new RuntimeException(String.format(
                        "Metadata store type not implemented: %s", metadataStoreConfiguration.getMetadataStoreType()));
        }
    }

}
