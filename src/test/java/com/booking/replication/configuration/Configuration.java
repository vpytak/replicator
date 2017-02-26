package com.booking.replication.configuration;

import com.booking.replication.util.Cmd;
import com.booking.replication.util.Duration;
import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import joptsimple.OptionSet;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by edmitriev on 2/26/17.
 */
public class Configuration {
    private static String configName = "sampleConfiguration.yaml";
    private static com.booking.replication.Configuration configuration;
    private static StartupParameters startupParameters;
    private static String[] args = new String[1];

    @Before
    public void setupConfiguration() throws IOException, ConfigurationException {
        if (configuration != null) return;
        args[0] = "-c" + configName;
        OptionSet optionSet = Cmd.parseArgs(args);
        startupParameters = new StartupParameters(optionSet);
        String  configPath = startupParameters.getConfigPath();

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream in = classLoader.getResourceAsStream(configPath);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        configuration = mapper.readValue(in, com.booking.replication.Configuration.class);
        configuration.loadStartupParameters(startupParameters);

        //System.out.println(startupParameters);
        System.out.println(configuration.toString());
    }

    @Test
    public void mainConfiguration() throws ConfigurationException {
        MainConfiguration mainConfiguration = configuration.getMainConfiguration();

        assertEquals("STDOUT", mainConfiguration.getApplierType());
        assertEquals(false, mainConfiguration.isDryRunMode());
        assertTrue(mainConfiguration == configuration.getMainConfiguration());
    }

    @Test
    public void replicationSchemaConfiguration() throws ConfigurationException {
        List<String> hostPool = new ArrayList<>();
        hostPool.add("localhost");

        ReplicationSchemaConfiguration replicationSchemaConfiguration = configuration.getReplicationSchemaConfiguration();

        assertEquals(hostPool, replicationSchemaConfiguration.getHostPool());
        assertEquals("password", replicationSchemaConfiguration.getPassword());
        assertTrue(replicationSchemaConfiguration == configuration.getReplicationSchemaConfiguration());
    }

    @Test
    public void mySQLFailoverConfiguration() throws ConfigurationException {
        MySQLFailoverConfiguration mySQLFailoverConfiguration = configuration.getMySQLFailoverConfiguration();
        assertEquals("(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})", mySQLFailoverConfiguration.getPgtid().getPattern());
        assertEquals("use `booking_meta`; ", mySQLFailoverConfiguration.getPgtid().getPrefix());
        assertTrue(mySQLFailoverConfiguration == configuration.getMySQLFailoverConfiguration());
    }

    @Test
    public void hBaseConfiguration() throws ConfigurationException {
        List<String> zookeeperQuorum = new ArrayList<>();
        zookeeperQuorum.add("localhost");

        HBaseConfiguration hBaseConfiguration = configuration.getHBaseConfiguration();
        assertEquals(false, hBaseConfiguration.isDryRunMode());
        assertEquals("rescore", hBaseConfiguration.getNamespace());
        assertEquals(zookeeperQuorum, hBaseConfiguration.getZookeeperConfiguration().getQuorum());
        assertTrue(hBaseConfiguration == configuration.getHBaseConfiguration());
    }

    @Test
    public void metadataStoreConfiguration() throws ConfigurationException {
        MetadataStoreConfiguration metadataStoreConfiguration = configuration.getMetadataStoreConfiguration();
        assertEquals("rescore_hbase_active_schema", metadataStoreConfiguration.getActiveSchemaDB());
        assertEquals("/hbase/replicator/rescore_hbase", metadataStoreConfiguration.getZookeeper().getPath());
        assertTrue(metadataStoreConfiguration == configuration.getMetadataStoreConfiguration());
    }

    @Test
    public void kafkaConfiguration() throws ConfigurationException {
        KafkaConfiguration kafkaConfiguration = configuration.getKafkaConfiguration();
        assertEquals("testTopic", kafkaConfiguration.getTopic());
        assertTrue(kafkaConfiguration == configuration.getKafkaConfiguration());
    }

    @Test
    public void validationConfiguration() throws ConfigurationException {
        ValidationConfiguration validationConfiguration = configuration.getValidationConfiguration();
        assertEquals("localhost:9092,localhost:9093", validationConfiguration.getBroker());
        assertTrue(validationConfiguration == configuration.getValidationConfiguration());
    }

    @Test
    public void metricsConfiguration() throws ConfigurationException {
        MetricsConfiguration metricsConfiguration = configuration.getMetricsConfiguration();
        assertEquals(Duration.parse("10 seconds"), metricsConfiguration.getFrequency());
        assertTrue(metricsConfiguration == configuration.getMetricsConfiguration());
    }
}
