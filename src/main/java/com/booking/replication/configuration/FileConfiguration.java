package com.booking.replication.configuration;

/**
 * Created by edmitriev on 2/15/17.
 */
public class FileConfiguration {
    public String path;

    public FileConfiguration() {
        if (path == null) {
            throw new RuntimeException("Metadata store set as file but no path is specified");
        }
    }

    public String getPath() {
        return path;
    }
}
