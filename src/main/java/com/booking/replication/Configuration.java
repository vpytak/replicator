package com.booking.replication;

import com.booking.replication.configuration.*;
import com.booking.replication.util.Duration;
import com.booking.replication.util.StartupParameters;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.naming.ConfigurationException;
import java.io.Serializable;
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
    private MainConfiguration mainConfiguration = null;

    public MainConfiguration getMainConfiguration() throws ConfigurationException {
        if (mainConfiguration == null) {
            mainConfiguration = new MainConfiguration(initialSnapshotMode, dryRunMode, startingBinlogPosition,
                    startingBinlogFileName, endingBinlogFileName, applierType);
        }
        return mainConfiguration;
    }

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
    private MySQLFailoverConfiguration mySQLFailoverConfiguration = null;

    private static class MySQLFailover {
        @JsonDeserialize
        PseudoGTIDConfiguration pgtid;
        @JsonDeserialize
        OrchestratorConfiguration orchestrator;
    }

    public MySQLFailoverConfiguration getMySQLFailoverConfiguration() throws ConfigurationException {
        if (mySQLFailoverConfiguration == null) {
            mySQLFailoverConfiguration = new MySQLFailoverConfiguration(mySQLFailover.pgtid, mySQLFailover.orchestrator);
        }
        return mySQLFailoverConfiguration;
    }


    @JsonDeserialize
    @JsonProperty("hbase")
    private HBase hbase;
    private HBaseConfiguration hbaseConfiguration = null;

    private static class HBase {
        String       namespace;
        List<String> zookeeper_quorum;
        boolean      writeRecentChangesToDeltaTables;
        @JsonDeserialize
        HiveImportsConfiguration hive_imports = new HiveImportsConfiguration();
    }

    public HBaseConfiguration getHBaseConfiguration() throws ConfigurationException {
        if (hbaseConfiguration == null) {
            hbaseConfiguration = new HBaseConfiguration(hbase.namespace, new ZookeeperConfiguration(hbase.zookeeper_quorum, "/"),
                    hbase.writeRecentChangesToDeltaTables, hbase.hive_imports, dryRunMode);
        }
        return hbaseConfiguration;
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
            kafkaConfiguration = new KafkaConfiguration(dryRunMode, kafka.broker, kafka.tables, kafka.excludetables, kafka.topic);
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
    public void loadStartupParameters(StartupParameters startupParameters ) throws ConfigurationException {
        dryRunMode = startupParameters.isDryrun();
        initialSnapshotMode = startupParameters.isInitialSnapshot();
        startingBinlogFileName = startupParameters.getBinlogFileName();
        startingBinlogPosition = startupParameters.getBinlogPosition();
        endingBinlogFileName   = startupParameters.getLastBinlogFileName();
        applierType = startupParameters.getApplier();

        // hbase specific parameters
        if (applierType.equals("hbase")) {
            if (hbase == null) throw new ConfigurationException("HBase not configured");

            hbase.writeRecentChangesToDeltaTables = startupParameters.isDeltaTables();
            if (startupParameters.getHbaseNamespace() != null) hbase.namespace = startupParameters.getHbaseNamespace();
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

}
