package com.booking.replication.util;

import joptsimple.OptionSet;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

/**
 * Created by bdevetak on 01/12/15.
 */
public class StartupParameters {

    private String  configPath;
    private String  schema;
    private String  applier;
    private String  binlogFileName;
    private Long    binlogPosition;
    private String  lastBinlogFileName;
    private boolean deltaTables;
    private boolean initialSnapshot;
    private String  hbaseNamespace;
    private boolean dryrun;

    public StartupParameters(OptionSet optionSet) {

        // use delta tables
        deltaTables = optionSet.has("delta");

        // run in dry-run mode
        dryrun = optionSet.has("dryrun");

        // initial snapshot mode
        initialSnapshot  = optionSet.has("initial-snapshot");

        // schema
        if (optionSet.hasArgument("schema")) {
            schema = optionSet.valueOf("schema").toString();
        } else {
            schema = "test";
        }

        // config-path
        configPath = (String) optionSet.valueOf("config-path");

        // applier, defaults to STDOUT
        applier = (String) optionSet.valueOf("applier");

        // setup hbase namespace
        hbaseNamespace = (String) optionSet.valueOf("hbase-namespace");

        // Start binlog filename
        binlogFileName = (String) optionSet.valueOf("binlog-filename");

        // Start binlog position
        binlogPosition = (Long) optionSet.valueOf("binlog-position");

        // Last binlog filename
        lastBinlogFileName = (String) optionSet.valueOf("last-binlog-filename");
    }

    public String getConfigPath() {
        return configPath;
    }

    public String getSchema() {
        return schema;
    }

    public String getApplier() {
        return applier;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public String getLastBinlogFileName() {
        return lastBinlogFileName;
    }

    public Long getBinlogPosition() {
        return binlogPosition;
    }

    public boolean isDeltaTables() {
        return deltaTables;
    }

    public boolean isDryrun() {
        return dryrun;
    }

    public boolean isInitialSnapshot() {
        return initialSnapshot;
    }

    public String getHbaseNamespace() {
        return hbaseNamespace;
    }

    @Override
    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("----------------------------------------------\n");
        stringBuffer.append("Parsed params:           \n");
        stringBuffer.append("\tconfig-path:           " + configPath + "\n");
        stringBuffer.append("\tschema:                " + schema + "\n");
        stringBuffer.append("\tapplier:               " + applier + "\n");
        stringBuffer.append("\tbinlog-filename:       " + binlogFileName + "\n");
        stringBuffer.append("\tposition:              " + binlogPosition + "\n");
        stringBuffer.append("\tlast-binlog-filename:  " + lastBinlogFileName + "\n");
        stringBuffer.append("\tinitial-snapshot:      " + initialSnapshot + "\n");
        stringBuffer.append("\thbase-namespace:       " + hbaseNamespace + "\n");
        stringBuffer.append("\tdry-run:               " + dryrun + "\n");
        stringBuffer.append("----------------------------------------------\n");
        return stringBuffer.toString();
    }
}
