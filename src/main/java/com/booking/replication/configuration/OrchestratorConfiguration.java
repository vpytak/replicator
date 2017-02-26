package com.booking.replication.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by edmitriev on 2/22/17.
 */
public class OrchestratorConfiguration {
    public String username;
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String password;
    public String url;
}