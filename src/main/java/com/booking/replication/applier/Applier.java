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

import java.io.IOException;

/**
 * Created by bosko on 11/14/15.
 */
public interface Applier {

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction)
            throws ApplierException, IOException;

//<<<<<<< HEAD
    void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction);

    void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction);

    void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction);
//=======
//    void applyCommitQueryEvent();
//
//    void applyXidEvent(RawBinlogEventXid event);
//>>>>>>> Migrating to binlog connector. Temporarily will support both parsers.

    void applyRotateEvent(RawBinlogEventRotate event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller);

    void forceFlush() throws ApplierException, IOException;

    void applyFormatDescriptionEvent(RawBinlogEventFormatDescription event);

    void applyTableMapEvent(RawBinlogEventTableMap event);

    void waitUntilAllRowsAreCommitted() throws IOException, ApplierException;

}
