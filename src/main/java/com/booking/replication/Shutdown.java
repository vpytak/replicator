package com.booking.replication;

import com.booking.replication.monitor.Overseer;
import com.booking.replication.monitor.ReplicatorHealthTrackerProxy;
import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.PipelineOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.util.concurrent.TimeUnit;

/**
 * Created by edmitriev on 2/25/17.
 */
public class Shutdown implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Shutdown.class);
    private final BinlogEventProducer binlogEventProducer;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final Overseer overseer;
    private final ReplicatorHealthTrackerProxy healthTracker;

    public Shutdown(BinlogEventProducer binlogEventProducer, PipelineOrchestrator pipelineOrchestrator, Overseer overseer, ReplicatorHealthTrackerProxy healthTracker) {
        this.binlogEventProducer = binlogEventProducer;
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.overseer = overseer;
        this.healthTracker = healthTracker;
    }

    @Override
    public void run() {
        LOGGER.info("Executing replicator shutdown hook...");
        stopOverseer();
        stopProducer();
        stopConsumer();
        stopSparkWebServer();
    }

    private void stopSparkWebServer() {
        try {
            LOGGER.info("Stopping the Spark web server...");
            Spark.stop(); // TODO: static stuff? Do we want to test this class?
            LOGGER.info("Stopped the Spark web server...");
        }
        catch (Exception e) {
            LOGGER.error("Failed to stop the Spark web server", e);
        }
    }

    private void stopConsumer() {
        try {
            LOGGER.info("Stopping Pipeline Orchestrator...");
            pipelineOrchestrator.setRunning(false);
            pipelineOrchestrator.join();
            LOGGER.info("Pipeline Orchestrator successfully stopped");
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted.", e);
        } catch (Exception e) {
            LOGGER.error("Failed to stop Pipeline Orchestrator", e);
        }
    }

    private void stopProducer() {
        try {
            // let open replicator stop its own threads
            if (binlogEventProducer.getOpenReplicator().isRunning()) {
                LOGGER.info("Stopping Producer...");
                binlogEventProducer.stop(10000, TimeUnit.MILLISECONDS);
                if (!binlogEventProducer.getOpenReplicator().isRunning()) {
                    LOGGER.info("Successfully stopped Producer thread");
                } else {
                    throw new Exception("Failed to stop Producer thread");
                }
            } else {
                LOGGER.info("Producer was allready stopped.");
            }
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted.", ie);
        } catch (Exception e) {
            LOGGER.error("Failed to stop Producer thread", e);
        }
    }

    private void stopOverseer() {
        try {
            LOGGER.info("Stopping Overseer...");
            overseer.stopMonitoring();
            overseer.join();
            healthTracker.stop();
            LOGGER.info("Overseer thread successfully stopped");
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted.", e);
        } catch (Exception e) {
            LOGGER.error("Failed to stop Overseer", e);
        }
    }
}
