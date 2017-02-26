package com.booking.replication.configuration;


import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Created by edmitriev on 2/22/17.
 */
public class MySQLFailoverConfiguration {
    private PseudoGTIDConfiguration pgtid;
    private OrchestratorConfiguration orchestrator;

    public MySQLFailoverConfiguration(PseudoGTIDConfiguration pgtid, OrchestratorConfiguration orchestrator) {
        this.pgtid = pgtid;
        this.orchestrator = orchestrator;
    }

    public PseudoGTIDConfiguration getPgtid() {
        return pgtid;
    }

    public OrchestratorConfiguration getOrchestrator() {
        return orchestrator;
    }

    public String getOrchestratorUserName() {
        return orchestrator.username;
    }

    public String getOrchestratorPassword() {
        return orchestrator.password;
    }

    public String getOrchestratorUrl() {
        return orchestrator.url;
    }
}
