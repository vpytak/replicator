package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
//<<<<<<< HEAD
import com.booking.replication.pipeline.CurrentTransaction;
//=======
//import com.booking.replication.binlog.event.RawBinlogEventFormatDescription;
//import com.booking.replication.binlog.event.RawBinlogEventRotate;
//import com.booking.replication.binlog.event.RawBinlogEventTableMap;
//import com.booking.replication.binlog.event.RawBinlogEventXid;
//>>>>>>> Migrating to binlog connector. Temporarily will support both parsers.
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Counter;

import java.io.IOException;

/**
 * Wraps an applier to count incoming events
 */
public class EventCountingApplier implements Applier {

    private final Applier wrapped;
    private final Counter counter;

    public EventCountingApplier(Applier wrapped, Counter counter)
        {
            if (wrapped == null)
            {
                throw new IllegalArgumentException("wrapped must not be null");
            }

            if (counter == null)
            {
                throw new IllegalArgumentException("counter must not be null");
            }

            this.wrapped = wrapped;
            this.counter = counter;
        }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction) throws ApplierException, IOException {
        wrapped.applyAugmentedRowsEvent(augmentedSingleRowEvent, currentTransaction);
        counter.inc();
    }

    @Override
//<<<<<<< HEAD
    public void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        wrapped.applyBeginQueryEvent(event, currentTransaction);
//=======
//    public void applyCommitQueryEvent() {
//        wrapped.applyCommitQueryEvent();
//>>>>>>> Migrating to binlog connector. Temporarily will support both parsers.
        counter.inc();
    }

    @Override
//<<<<<<< HEAD
    public void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        wrapped.applyCommitQueryEvent(event, currentTransaction);
        counter.inc();
    }

    @Override
    public void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction) {
        wrapped.applyXidEvent(event, currentTransaction);
//=======
//    public void applyXidEvent(RawBinlogEventXid event) {
//        wrapped.applyXidEvent(event);
//>>>>>>> Migrating to binlog connector. Temporarily will support both parsers.
        counter.inc();
    }

    @Override
    public void applyRotateEvent(RawBinlogEventRotate event) throws ApplierException, IOException {
        wrapped.applyRotateEvent(event);
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {
        wrapped.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, caller);
        counter.inc();
    }

    @Override
    public void forceFlush() throws ApplierException, IOException {
        wrapped.forceFlush();
    }

    @Override
    public void applyFormatDescriptionEvent(RawBinlogEventFormatDescription event) {
        wrapped.applyFormatDescriptionEvent(event);
        counter.inc();
    }

    @Override
    public void applyTableMapEvent(RawBinlogEventTableMap event) {
        wrapped.applyTableMapEvent(event);
        counter.inc();
    }

    @Override
    public void waitUntilAllRowsAreCommitted() throws IOException, ApplierException {
        wrapped.waitUntilAllRowsAreCommitted();
    }
}
