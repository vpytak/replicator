package com.booking.replication;

import com.booking.replication.applier.*;
import com.booking.replication.applier.kafka.ApplierFactory;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.configuration.*;
import com.booking.replication.metrics.MainProgressCounter;
import com.booking.replication.monitor.*;
import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.replicant.ReplicantPool;

import com.booking.replication.sql.QueryInspector;
import com.booking.replication.util.BinlogCoordinatesFinder;
import com.booking.replication.validation.ValidationService;
import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;


/**
 * Booking replicator. Has two main objects (producer and consumer)
 * that reference the same thread-safe queue.
 * Producer pushes binlog events to the queue and consumer
 * reads them. Producer is basically a wrapper for open replicator,
 * and consumer is wrapper for all booking specific logic (schema
 * version control, augmenting events and storing events).
 */
public class Replicator {

    private final BinlogEventProducer  binlogEventProducer;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final Overseer             overseer;
    private final ReplicantPool        replicantPool;
    private final PipelinePosition     pipelinePosition;
    private final ReplicatorHealthTrackerProxy healthTracker;

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    public Replicator(MainConfiguration mainConfiguration,
                      MySQLFailoverConfiguration mySQLFailoverConfiguration,
                      ReplicationSchemaConfiguration replicationSchemaConfiguration,
                      HBaseConfiguration hBaseConfiguration,
                      MetadataStoreConfiguration metadataStoreConfiguration,
                      KafkaConfiguration kafkaConfiguration,
                      ValidationConfiguration validationConfiguration,
                      ReplicatorHealthTrackerProxy healthTracker,
                      Counter interestingEventsObservedCounter
    ) throws Exception {

        this.healthTracker = healthTracker;
        replicantPool = new ReplicantPool(replicationSchemaConfiguration.getHostPool(), replicationSchemaConfiguration);
        pipelinePosition = getPosition(mainConfiguration, mySQLFailoverConfiguration, replicationSchemaConfiguration);

        if (!validatePosition(pipelinePosition, replicantPool, mainConfiguration)) {
            LOGGER.error(String.format(
                    "The current position is beyond the last position you configured.\nThe current position is: %s %s",
                    pipelinePosition.getStartPosition().getBinlogFilename(),
                    pipelinePosition.getStartPosition().getBinlogPosition())
            );
            System.exit(1);
        }


        LOGGER.info(String.format(
                "Starting replication from: %s %s",
                pipelinePosition.getStartPosition().getBinlogFilename(),
                pipelinePosition.getStartPosition().getBinlogPosition()));


        ReplicatorQueues replicatorQueues = new ReplicatorQueues();

        binlogEventProducer = new BinlogEventProducer(replicatorQueues.rawQueue, pipelinePosition,
                replicationSchemaConfiguration, replicantPool);

        ValidationService validationService = ValidationService.getInstance(validationConfiguration);

        MainProgressCounter mainProgressCounter = MainProgressCounter.getInstance(mainConfiguration.getApplierType());

        healthTracker.setTrackerImplementation(getReplicatorHealthTracker(interestingEventsObservedCounter, mainProgressCounter));

        Applier eventCountingApplier = new EventCountingApplier(
                getApplier(mainConfiguration, replicationSchemaConfiguration, hBaseConfiguration, kafkaConfiguration,
                        validationConfiguration, validationService, mainProgressCounter),
                interestingEventsObservedCounter);


        pipelineOrchestrator = new PipelineOrchestrator(mainConfiguration, metadataStoreConfiguration, mySQLFailoverConfiguration,
                replicationSchemaConfiguration, replicatorQueues, pipelinePosition, eventCountingApplier, replicantPool
        );

        overseer = new Overseer(binlogEventProducer, pipelineOrchestrator, pipelinePosition);
    }

    private IReplicatorHealthTracker getReplicatorHealthTracker(Counter interestingEventsObservedCounter, MainProgressCounter mainProgressCounter) {
        if (mainProgressCounter == null) return new ReplicatorHealthTrackerDummy();

        return new ReplicatorHealthTracker(
                new ReplicatorDoctor(mainProgressCounter.getCounter(), mainProgressCounter.getDescription(), interestingEventsObservedCounter),
                600);
    }

    private Applier getApplier(MainConfiguration mainConfiguration, ReplicationSchemaConfiguration replicationSchemaConfiguration, HBaseConfiguration hBaseConfiguration, KafkaConfiguration kafkaConfiguration, ValidationConfiguration validationConfiguration, ValidationService validationService, MainProgressCounter mainProgressCounter) throws IOException {
        switch (mainConfiguration.getApplierType()) {
            case "STDOUT":
                return ApplierFactory.getApplier();
            case "hbase":
                return ApplierFactory.getApplier(mainConfiguration, hBaseConfiguration, replicationSchemaConfiguration, validationConfiguration,
                        mainProgressCounter.getCounter(), validationService);
            case "kafka":
                return ApplierFactory.getApplier(kafkaConfiguration, mainProgressCounter.getCounter());
            default:
                throw new RuntimeException(String.format("Unknown applier: %s", mainConfiguration.getApplierType()));
        }
    }

    private boolean validatePosition(PipelinePosition pipelinePosition, ReplicantPool replicantPool, MainConfiguration mainConfiguration) throws Exception {
        return !(mainConfiguration.getEndingBinlogFileName() != null
                && pipelinePosition.getStartPosition().greaterThan(new BinlogPositionInfo(
                replicantPool.getReplicantDBActiveHost(),
                replicantPool.getReplicantDBActiveHostServerID(),
                mainConfiguration.getEndingBinlogFileName(), 4L)));
    }

    private PipelinePosition getPosition(MainConfiguration mainConfiguration, MySQLFailoverConfiguration mySQLFailoverConfiguration, ReplicationSchemaConfiguration replicationSchemaConfiguration) throws SQLException {
        PipelinePosition pipelinePosition;
        if (mySQLFailoverConfiguration != null) {

            // mysql high-availability mode
            if (mainConfiguration.getStartingBinlogFileName() != null) {
                pipelinePosition = getPositionHighAvailabilityMode(mainConfiguration, replicationSchemaConfiguration);
            } else {
                // failover:
                //  1. get Pseudo GTID from safe checkpoint
                //  2. get active host from the pool
                //  3. get binlog-filename and binlog-position that correspond to
                //     Pseudo GTID on the active host (this is done by calling the
                //     MySQL Orchestrator http API (https://github.com/outbrain/orchestrator).
                LastCommittedPositionCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();

                if ( safeCheckPoint != null ) {
                    String pseudoGTID = safeCheckPoint.getPseudoGTID();
                    if (pseudoGTID != null) {
                        pipelinePosition = getPositionPseudoGTID(mySQLFailoverConfiguration, replicationSchemaConfiguration, safeCheckPoint, pseudoGTID);
                    } else {
                        pipelinePosition = getPositionHostSpecific(safeCheckPoint);
                    }
                } else {
                    throw new RuntimeException("Could not load safe check point.");
                }

            }
        } else {
            if (mainConfiguration.getStartingBinlogFileName() != null) {
                // single replicant mode
                pipelinePosition = getPositionSingleReplicantMode(mainConfiguration, replicationSchemaConfiguration);
            } else {
                // metadata from coordinator
                LOGGER.info("Start binlog not specified, reading metadata from coordinator");
                LastCommittedPositionCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();
                if ( safeCheckPoint != null ) {
                    pipelinePosition = getPositionSafeCheckPoint(safeCheckPoint);
                } else {
                    throw new RuntimeException("Could not find start binlog in metadata or startup options");
                }
            }
        }
        return pipelinePosition;
    }

    private PipelinePosition getPositionHighAvailabilityMode(MainConfiguration mainConfiguration, ReplicationSchemaConfiguration replicationSchemaConfiguration) {
        // TODO: make mandatory to specify host name when starting from specific binlog file
        // At the moment the first host in the pool list is assumed when starting with
        // specified binlog-file
        String mysqlHost = replicationSchemaConfiguration.getHostPool().get(0);
        int serverID     = replicantPool.getReplicantDBActiveHostServerID();

        LOGGER.info(String.format("Starting replicator in high-availability mode with: "
                + "mysql-host %s, server-id %s, binlog-filename %s",
                mysqlHost, serverID, mainConfiguration.getStartingBinlogFileName()));

        return new PipelinePosition(
                mysqlHost,
                serverID,
                mainConfiguration.getStartingBinlogFileName(),
                mainConfiguration.getStartingBinlogPosition(),
                mainConfiguration.getStartingBinlogFileName(),
                4L
        );
    }

    private PipelinePosition getPositionSafeCheckPoint(LastCommittedPositionCheckpoint safeCheckPoint) {
        String mysqlHost      = safeCheckPoint.getHostName();
        int    serverID       = safeCheckPoint.getSlaveId();
        String lastVerifiedBinlogFileName = safeCheckPoint.getLastVerifiedBinlogFileName();
        Long   lastVerifiedBinlogPosition = safeCheckPoint.getLastVerifiedBinlogPosition();

        LOGGER.info(
                "Got safe checkpoint from coordinator: "
                        + "{ mysqlHost       => " + mysqlHost
                        + "{ serverID        => " + serverID
                        + "{ binlog-file     => " + lastVerifiedBinlogFileName
                        + ", binlog-position => " + lastVerifiedBinlogPosition
                        + " }"
        );

        // starting from checkpoint position, so startPosition := lastVerifiedPosition
        return new PipelinePosition(
            mysqlHost,
            serverID,
            lastVerifiedBinlogFileName,
            lastVerifiedBinlogPosition,
            lastVerifiedBinlogFileName,
            lastVerifiedBinlogPosition
        );
    }

    private PipelinePosition getPositionSingleReplicantMode(MainConfiguration mainConfiguration, ReplicationSchemaConfiguration replicationSchemaConfiguration) {
        // TODO: make mandatory to specify host name when starting from specific binlog file
        // At the moment the first host in the pool list is assumed when starting with
        // specified binlog-file

        String mysqlHost = replicationSchemaConfiguration.getHostPool().get(0);
        int serverID     = replicantPool.getReplicantDBActiveHostServerID();

        LOGGER.info(String.format("Starting replicator in single-replicant mode with: "
            + "mysql-host %s, server-id %s, binlog-filename %s",
            mysqlHost, serverID, mainConfiguration.getStartingBinlogFileName()));

        return new PipelinePosition(
            mysqlHost,
            serverID,
            mainConfiguration.getStartingBinlogFileName(),
            mainConfiguration.getStartingBinlogPosition(),
            mainConfiguration.getStartingBinlogFileName(),
            4L
        );
    }

    private PipelinePosition getPositionHostSpecific(LastCommittedPositionCheckpoint safeCheckPoint) throws SQLException {
        // the binlog file name and position are host specific which means that
        // we can't get mysql host from the pool. We must use the host from the
        // safe checkpoint. If that host is not avaiable then, without pGTID,
        // failover can not be done and the replicator will exit with SQLException.
        String mysqlHost = safeCheckPoint.getHostName();
        int    serverID  = replicantPool.obtainServerID(mysqlHost);

        LOGGER.warn("PsuedoGTID not available in safe checkpoint. "
                + "Defaulting back to host-specific binlog coordinates.");

        String startingBinlogFileName = safeCheckPoint.getLastVerifiedBinlogFileName();
        Long   startingBinlogPosition = safeCheckPoint.getLastVerifiedBinlogPosition();

        return new PipelinePosition(
            mysqlHost,
            serverID,
            startingBinlogFileName,
            startingBinlogPosition,
            safeCheckPoint.getLastVerifiedBinlogFileName(),
            safeCheckPoint.getLastVerifiedBinlogPosition()
        );
    }

    private PipelinePosition getPositionPseudoGTID(MySQLFailoverConfiguration mySQLFailoverConfiguration, ReplicationSchemaConfiguration replicationSchemaConfiguration, LastCommittedPositionCheckpoint safeCheckPoint, String pseudoGTID) {
        String replicantActiveHost = replicantPool.getReplicantDBActiveHost();
        int    serverID            = replicantPool.getReplicantDBActiveHostServerID();
        boolean sameHost = replicantActiveHost.equals(safeCheckPoint.getHostName());

        LOGGER.info("found pseudoGTID in safe checkpoint: " + pseudoGTID);

        BinlogCoordinatesFinder coordinatesFinder = new BinlogCoordinatesFinder(
                replicantActiveHost,
                3306,
                replicationSchemaConfiguration.getUsername(),
                replicationSchemaConfiguration.getPassword(),
                new QueryInspector(mySQLFailoverConfiguration.getPgtid().getPattern()));

        BinlogCoordinatesFinder.BinlogCoordinates coordinates = coordinatesFinder.findCoordinates(pseudoGTID);

        String startingBinlogFileName = coordinates.getFileName();
        Long   startingBinlogPosition = coordinates.getPosition();

        LOGGER.info("PseudoGTID resolved to: " + startingBinlogFileName + ":" + startingBinlogPosition);

        return new PipelinePosition(
            replicantActiveHost,
            serverID,
            startingBinlogFileName,
            startingBinlogPosition,
            // positions are not comparable between different hosts
            (sameHost ? safeCheckPoint.getLastVerifiedBinlogFileName() : startingBinlogFileName),
            (sameHost ? safeCheckPoint.getLastVerifiedBinlogPosition() : startingBinlogPosition)
        );
    }

    // start()
    public void start() throws Exception {

        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                new Shutdown(binlogEventProducer, pipelineOrchestrator, overseer, healthTracker)));

        // Start up
        binlogEventProducer.start();
        pipelineOrchestrator.start();
        overseer.start();

        healthTracker.start();

        while (!pipelineOrchestrator.isReplicatorShutdownRequested()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                LOGGER.error("Main thread interrupted with: ", ie);
                pipelineOrchestrator.requestReplicatorShutdown();
            }
        }

        System.exit(0);
    }
}
