package com.booking.replication.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.naming.ConfigurationException;
import java.io.Serializable;
import java.util.List;

/**
 * Created by edmitriev on 2/16/17.
 */
public class ReplicationSchemaConfiguration implements Serializable{
    private String name;
    private String username;
    private String password;
    private List<String> hostPool;
    private int port;


    public ReplicationSchemaConfiguration(String name, String username, String password, List<String> hostPool, int port) throws ConfigurationException {
        this.name = name;
        this.username = username;
        this.password = password;
        this.hostPool = hostPool;
        this.port = port;

        if (name == null) throw new ConfigurationException("Replication schema name cannot be null.");
        if (hostPool == null) throw new ConfigurationException("Replication schema hostPool cannot be null.");
        if (username == null) throw new ConfigurationException("Replication schema user name cannot be null.");
    }

    public String getName() {
        return name;
    }

    public String getUsername() {
        return username;
    }

    @JsonIgnore
    public String getPassword() {
        return password;
    }

    public List<String> getHostPool() {
        return hostPool;
    }

    public int getPort() {
        return port;
    }
}
