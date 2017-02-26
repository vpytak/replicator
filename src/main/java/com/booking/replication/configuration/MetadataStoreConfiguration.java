package com.booking.replication.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.naming.ConfigurationException;

/**
 * Created by edmitriev on 2/15/17.
 */
public class MetadataStoreConfiguration {
    public static final int METADATASTORE_ZOOKEEPER = 1;
    public static final int METADATASTORE_FILE = 2;

    private String username;
    private String password;
    private String host;
    private String database;
    private ZookeeperConfiguration zookeeper;
    private FileConfiguration file;

    public MetadataStoreConfiguration(String username, String password, String host, String database,
                                      ZookeeperConfiguration zookeeper, FileConfiguration file) throws ConfigurationException {
        this.username = username;
        this.password = password;
        this.host = host;
        this.database = database;
        this.zookeeper = zookeeper;
        this.file = file;

        if (zookeeper == null && file == null) {
            throw new ConfigurationException("No metadata store specified, please provide "
                    + "either zookeeper or file-based metadata storage.");
        }
    }

    public int getMetadataStoreType() {
        if (zookeeper != null) {
            return METADATASTORE_ZOOKEEPER;
        } else if (file != null) {
            return METADATASTORE_FILE;
        } else {
            throw new RuntimeException("Metadata store not configured, please define a zookeeper or file metadata store.");
        }
    }

    public String getActiveSchemaDSN() {
        return String.format("jdbc:mysql://%s/%s", host, database);
    }

    public String getActiveSchemaHost() {
        return host;
    }

    public String getActiveSchemaUserName() {
        return username;
    }

    public String getActiveSchemaPassword() {
        return password;
    }

    public String getActiveSchemaDB() {
        return database;
    }

    public ZookeeperConfiguration getZookeeper() {
        return zookeeper;
    }

    public FileConfiguration getFile() {
        return file;
    }
}
