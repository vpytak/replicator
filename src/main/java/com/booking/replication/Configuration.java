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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    private boolean initialSnapshotMode;
    private boolean dryRunMode;
    private long    startingBinlogPosition;
    private String  startingBinlogFileName;
    private String  endingBinlogFileName;
    private String  applierType;
    @JsonIgnore
    private MainConfiguration mainConfiguration = null;

    public MainConfiguration getMainConfiguration() throws ConfigurationException {
        if (mainConfiguration == null) {
            mainConfiguration = new MainConfiguration(initialSnapshotMode, dryRunMode, startingBinlogPosition,
                    startingBinlogFileName, endingBinlogFileName, applierType);
        }
        return mainConfiguration;
    }

    private static class ReplicationSchema implements Serializable {
        public String name;
        public String username;
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
        public String password;
        public List<String> host_pool;
        public int port = 3306;
    }

    @JsonDeserialize
    @JsonProperty("replication_schema")
    private ReplicationSchema replicationSchema;
    @JsonIgnore
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
    @JsonIgnore
    private MySQLFailoverConfiguration mySQLFailoverConfiguration = null;

    private static class MySQLFailover {
        @JsonDeserialize
        public PseudoGTIDConfiguration pgtid;
        @JsonDeserialize
        public OrchestratorConfiguration orchestrator;
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
    @JsonIgnore
    private HBaseConfiguration hbaseConfiguration = null;

    private static class HBase {
        public String       namespace;
        public List<String> zookeeper_quorum;
        public boolean      writeRecentChangesToDeltaTables;
        @JsonDeserialize
        public HiveImportsConfiguration hive_imports = new HiveImportsConfiguration();
    }

    public HBaseConfiguration getHBaseConfiguration() throws ConfigurationException {
        if (hbaseConfiguration == null) {
            hbaseConfiguration = new HBaseConfiguration(hbase.namespace, new ZookeeperConfiguration(hbase.zookeeper_quorum, "/"),
                    hbase.writeRecentChangesToDeltaTables, hbase.hive_imports, dryRunMode);
        }
        return hbaseConfiguration;
    }


    @JsonDeserialize
    @JsonProperty("metadata_store")
    private MetadataStore metadataStore;
    @JsonIgnore
    private MetadataStoreConfiguration metadataStoreConfiguration = null;

    private static class MetadataStore {
        public String       username;
        @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
        public String       password;
        public String       host;
        public String       database;
        @JsonDeserialize
        public ZookeeperConfiguration zookeeper;
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
    @JsonIgnore
    private KafkaConfiguration kafkaConfiguration;

    private static class Kafka {
        public String broker;
        public List<String> tables;
        public List<String> excludetables;
        public String topic;
    }

    public KafkaConfiguration getKafkaConfiguration(){
        if (kafkaConfiguration == null) {
            kafkaConfiguration = new KafkaConfiguration(dryRunMode, kafka.broker, kafka.tables, kafka.excludetables, kafka.topic);
        }
        return kafkaConfiguration;
    }


    private static class Validation {
        public String broker;
        public String topic;
        public String tag = "general";
        @JsonProperty("source_domain")
        public String sourceDomain;
        @JsonProperty("target_domain")
        public String targetDomain;
        public long throttling = TimeUnit.SECONDS.toMillis(5);;
    }

    @JsonDeserialize
    @JsonProperty("validation")
    private Validation validation;
    @JsonIgnore
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
        public Duration     frequency;
        @JsonDeserialize
        public HashMap<String, MetricsReporterConfiguration> reporters = new HashMap<>();
    }

    @JsonDeserialize()
    public Metrics metrics = new Metrics();
    @JsonIgnore
    private MetricsConfiguration metricsConfiguration = null;

    public MetricsConfiguration getMetricsConfiguration() throws ConfigurationException {
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
    public void loadStartupParameters(StartupParameters startupParameters) throws ConfigurationException {
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
            LOGGER.warn("Can't serialize configuration: ", e);
        }
        return "";
    }

}
