package com.booking.replication.configuration;

/**
 * Created by edmitriev on 2/22/17.
 */
public class MainConfiguration {
    private boolean initialSnapshotMode;
    private boolean dryRunMode;
    private long    startingBinlogPosition;
    private String  startingBinlogFileName;
    private String  endingBinlogFileName;
    private String  applierType;

    public MainConfiguration(boolean initialSnapshotMode, boolean dryRunMode, long startingBinlogPosition, String startingBinlogFileName, String endingBinlogFileName, String applierType) {
        this.initialSnapshotMode = initialSnapshotMode;
        this.dryRunMode = dryRunMode;
        this.startingBinlogPosition = startingBinlogPosition;
        this.startingBinlogFileName = startingBinlogFileName;
        this.endingBinlogFileName = endingBinlogFileName;
        this.applierType = applierType;
    }

    public boolean isInitialSnapshotMode() {
        return initialSnapshotMode;
    }

    public boolean isDryRunMode() {
        return dryRunMode;
    }

    public long getStartingBinlogPosition() {
        return startingBinlogPosition;
    }

    public String getStartingBinlogFileName() {
        return startingBinlogFileName;
    }

    public String getEndingBinlogFileName() {
        return endingBinlogFileName;
    }

    public String getApplierType() {
        return applierType;
    }
}
