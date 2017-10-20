package com.booking.replication.pipeline.event.handler;

import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by edmitriev on 7/12/17.
 */
public class FormatDescriptionEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FormatDescriptionEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final PipelineOrchestrator pipelineOrchestrator;

    public FormatDescriptionEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }


    @Override
    public void apply(RawBinlogEvent binlogEventV4, CurrentTransaction currentTransaction) {
        final FormatDescriptionEvent event = (FormatDescriptionEvent) binlogEventV4;
        eventHandlerConfiguration.getApplier().applyFormatDescriptionEvent(event);
    }

    @Override
    public void handle(RawBinlogEvent binlogEventV4) throws TransactionException, TransactionSizeLimitException {
        final FormatDescriptionEvent event = (FormatDescriptionEvent) binlogEventV4;
        if (pipelineOrchestrator.isInTransaction()) {
            pipelineOrchestrator.addEventIntoTransaction(event);
        } else {
            pipelineOrchestrator.beginTransaction();
            pipelineOrchestrator.addEventIntoTransaction(event);
            pipelineOrchestrator.commitTransaction(event.getHeader().getTimestamp(), CurrentTransaction.FAKEXID);
        }
    }
}
