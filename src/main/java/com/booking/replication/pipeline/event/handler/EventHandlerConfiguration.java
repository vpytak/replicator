package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.sql.QueryInspector;

/**
 * Created by edmitriev on 7/19/17.
 */
public class EventHandlerConfiguration {
    private Applier applier;
    private EventAugmenter eventAugmenter;
    private PipelinePosition pipelinePosition;
    private PipelineOrchestrator pipelineOrchestrator;

    public EventHandlerConfiguration(Applier applier, EventAugmenter eventAugmenter, PipelinePosition pipelinePosition, PipelineOrchestrator pipelineOrchestrator) {
        this.applier = applier;
        this.eventAugmenter = eventAugmenter;
        this.pipelinePosition = pipelinePosition;
        this.pipelineOrchestrator = pipelineOrchestrator;
    }

    public Applier getApplier() {
        return applier;
    }

    public EventAugmenter getEventAugmenter() {
        return eventAugmenter;
    }

    public PipelinePosition getPipelinePosition() {
        return pipelinePosition;
    }

    public PipelineOrchestrator getPipelineOrchestrator() { return  pipelineOrchestrator; }

}
