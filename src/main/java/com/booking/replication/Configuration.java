package com.booking.replication;

import com.booking.replication.configuration.*;
import com.booking.replication.util.Duration;
import com.booking.replication.util.StartupParameters;
import com.google.common.base.Joiner;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.naming.ConfigurationException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Configuration instance.
 *
 * <p>This object is instantiated by deserializing from a yaml config file.</p>
 */
public class Configuration {

    private boolean initialSnapshotMode;
    private boolean dryRunMode;
    private long    startingBinlogPosition;
    private String  startingBinlogFileName;
    private String  endingBinlogFileName;
    private String  applierType;

    private static class ReplicationSchema implements Serializable {
        private String name;
        private String username;
        private String password;
        private List<String> host_pool;
        private int port = 3306;
    }

    @JsonDeserialize
    private ReplicationSchema replicationSchema;
    private ReplicationSchemaConfiguration replicationSchemaConfiguration = null;

    public ReplicationSchemaConfiguration getReplicationSchemaConfiguration() throws ConfigurationException {
        if (replicationSchemaConfiguration == null) {
            replicationSchemaConfiguration = new ReplicationSchemaConfiguration(replicationSchema.name,
                    replicationSchema.username, replicationSchema.password, replicationSchema.host_pool,
                    replicationSchema.port);
        }
        return replicationSchemaConfiguration;
    }






    @JsonDeserialize
    @JsonProperty("mysql_failover")
    private MySQLFailover mySQLFailover;

    private static class MySQLFailover {

        @JsonDeserialize
        PseudoGTIDConfig pgtid;

        private static class PseudoGTIDConfig implements Serializable {
            String p_gtid_pattern;
            String p_gtid_prefix;
        }

        @JsonDeserialize
        Orchestrator orchestrator;

        private static class Orchestrator {
            String username;
            String password;
            String url;
        }
    }

    @JsonDeserialize
    @JsonProperty("hbase")
    private HBaseConfiguration hbaseConfiguration;

    private static class HBaseConfiguration {

        String       namespace;
        List<String> zookeeper_quorum;
        boolean      writeRecentChangesToDeltaTables;

        @JsonDeserialize
        HiveImports     hive_imports = new HiveImports();

        private static class HiveImports {
            public List<String> tables = Collections.emptyList();
        }
    }








    @JsonDeserialize
    private MetadataStore metadataStore;
    private MetadataStoreConfiguration metadataStoreConfiguration = null;

    private static class MetadataStore {
        String       username;
        String       password;
        public String       host;
        String       database;
        @JsonDeserialize
        ZookeeperConfiguration zookeeper;
        @JsonDeserialize
        public FileConfiguration file;
    }

    /**
     * Metadata store type.
     *
     * @return Zookeeper/File
     */
    public MetadataStoreConfiguration getMetadataStoreConfiguration() throws ConfigurationException {
        if (metadataStoreConfiguration == null) {
            metadataStoreConfiguration = new MetadataStoreConfiguration(metadataStore.username, metadataStore.password,
                    metadataStore.host, metadataStore.database, metadataStore.zookeeper, metadataStore.file);
        }
        return metadataStoreConfiguration;
    }

    @JsonDeserialize
    private Kafka kafka;
    private KafkaConfiguration kafkaConfiguration;

    private static class Kafka {
        String broker;
        List<String> tables;
        List<String> excludetables;
        String topic;
    }

    public KafkaConfiguration getKafkaConfiguration(){
        if (kafkaConfiguration == null) {
            kafkaConfiguration = new KafkaConfiguration(isDryRunMode(), kafka.broker, kafka.tables, kafka.excludetables, kafka.topic);
        }
        return kafkaConfiguration;
    }

    private static class Validation {
        private String broker;
        private String topic;
        private String tag = "general";
        @JsonProperty("source_domain")
        private String sourceDomain;
        @JsonProperty("target_domain")
        private String targetDomain;
        private long throttling = TimeUnit.SECONDS.toMillis(5);;
    }

    @JsonDeserialize
    @JsonProperty("validation")
    private Validation validation;
    private ValidationConfiguration validationConfiguration = null;

    public ValidationConfiguration getValidationConfiguration(){
        if (validationConfiguration == null) {
            validationConfiguration = new ValidationConfiguration(
                    validation.broker, validation.topic, validation.tag, validation.sourceDomain,
                    validation.targetDomain, validation.throttling);
        }
        return validationConfiguration;
    }

    public static class Metrics {
        Duration     frequency;
        @JsonDeserialize
        HashMap<String, MetricsReporterConfiguration> reporters = new HashMap<>();
    }

    @JsonDeserialize()
    public Metrics metrics = new Metrics();
    private MetricsConfiguration metricsConfiguration = null;

    MetricsConfiguration getMetricsConfiguration() throws ConfigurationException {
        if (metrics.frequency == null) metrics.frequency = Duration.parse("10 seconds");
        if (metricsConfiguration == null) {
            metricsConfiguration = new MetricsConfiguration(metrics.frequency, metrics.reporters);
        }
        return metricsConfiguration;
    }










    /**
     * Apply command line parameters to the configuration object.
     *
     * @param startupParameters     Startup parameters
     */
    public void loadStartupParameters(StartupParameters startupParameters ) {

        applierType = startupParameters.getApplier();

        if (applierType.equals("hbase") && hbaseConfiguration == null) {
            throw new RuntimeException("HBase not configured");
        }

        // staring position
        startingBinlogFileName = startupParameters.getBinlogFileName();
        startingBinlogPosition = startupParameters.getBinlogPosition();
        endingBinlogFileName   = startupParameters.getLastBinlogFileName();

        // hbase specific parameters
        if (applierType.equals("hbase") && hbaseConfiguration != null) {
            // delta tables
            hbaseConfiguration.writeRecentChangesToDeltaTables = startupParameters.isDeltaTables();
            // namespace
            if (startupParameters.getHbaseNamespace() != null) {
                hbaseConfiguration.namespace = startupParameters.getHbaseNamespace();
            }
        }

        // initial snapshot mode
        initialSnapshotMode = startupParameters.isInitialSnapshot();

        dryRunMode = startupParameters.isDryrun();

    }

    /**
     * Validate configuration.
     */
    public void validate() {
        if (applierType.equals("hbase")) {
            if (hbaseConfiguration.namespace == null) {
                throw new RuntimeException("HBase namespace cannot be null.");
            }
        }
    }

    /**
     * Serialize configuration.
     *
     * @return String Serialized configuration
     */
    public String toString() {
        try {
            return new ObjectMapper(new YAMLFactory()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    // =========================================================================
    // Replication schema config getters
    public int getReplicantPort() {
        return replicationSchema.port;
    }

    public List<String> getReplicantDBHostPool() {
        return this.replicationSchema.host_pool;
    }

    public String getReplicantSchemaName() {
        return replicationSchema.name;
    }

    public String getReplicantDBUserName() { return replicationSchema.username; }

    @JsonIgnore
    public String getReplicantDBPassword() {
        return replicationSchema.password;
    }

    // =========================================================================
    // Orchestrator config getters
    public String getOrchestratorUserName() {
        return mySQLFailover.orchestrator.username;
    }

    @JsonIgnore
    public String getOrchestratorPassword() {
        return mySQLFailover.orchestrator.password;
    }

    public String getOrchestratorUrl() {
        return mySQLFailover.orchestrator.url;
    }

    public MySQLFailover getMySQLFailover() {
        return  mySQLFailover;
    }

    // =========================================================================
    // Binlog file names and position getters
    public String getStartingBinlogFileName() {
        return startingBinlogFileName;
    }

    public String getLastBinlogFileName() {
        return endingBinlogFileName;
    }

    public long getStartingBinlogPosition() {
        return startingBinlogPosition;
    }

    // =========================================================================
    // Applier type
    public String getApplierType() {
        return applierType;
    }


    // ========================================================================
    // Metadata store config getters
    public String getActiveSchemaDSN() {
        return String.format("jdbc:mysql://%s/%s", metadataStore.host, metadataStore.database);
    }

    public String getActiveSchemaHost() {
        return metadataStore.host;
    }

    public String getActiveSchemaUserName() {
        return metadataStore.username;
    }

    @JsonIgnore
    public String getActiveSchemaPassword() {
        return metadataStore.password;
    }

    public String getActiveSchemaDB() {
        return metadataStore.database;
    }

    public String getpGTIDPattern() {
        return mySQLFailover.pgtid.p_gtid_pattern;
    }


    public String getpGTIDPrefix() {
        return mySQLFailover.pgtid.p_gtid_prefix;
    }

    /**
     * Get initial snapshot mode flag.
     */
    public boolean isInitialSnapshotMode() {
        return initialSnapshotMode;
    }

    /**
     * HBase configuration getters.
     */
    public String getHbaseNamespace() {
        if (hbaseConfiguration != null) {
            return hbaseConfiguration.namespace;
        } else {
            return null;
        }
    }

    public Boolean isWriteRecentChangesToDeltaTables() {
        if (hbaseConfiguration != null) {
            return hbaseConfiguration.writeRecentChangesToDeltaTables;
        } else {
            return null;
        }
    }

    public List<String> getTablesForWhichToTrackDailyChanges() {
        if (hbaseConfiguration != null) {
            return hbaseConfiguration.hive_imports.tables;
        } else {
            return null;
        }
    }

    public String getHBaseQuorum() {
        if (hbaseConfiguration != null) {
            return Joiner.on(",").join(hbaseConfiguration.zookeeper_quorum);
        } else {
            return null;
        }
    }

    public boolean isDryRunMode() {
        return dryRunMode;
    }


}
