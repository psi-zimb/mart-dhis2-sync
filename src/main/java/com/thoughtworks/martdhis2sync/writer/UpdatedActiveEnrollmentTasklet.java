package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpdatedActiveEnrollmentTasklet implements Tasklet {

    @Autowired
    private TrackersHandler trackersHandler;


    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "UPDATED ACTIVE ENROLLMENT TASKLET: ";

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String user = chunkContext.getStepContext().getJobParameters().get("user").toString();
        updateTrackers(user);
        return RepeatStatus.FINISHED;
    }

    private void updateTrackers(String user) {
        if (!EnrollmentUtil.enrollmentsToSaveInTracker.isEmpty()) {
            trackersHandler.updateInEnrollmentTracker(user, LOG_PREFIX, logger);
        }
        if (!EventUtil.eventsToSaveInTracker.isEmpty()) {
            trackersHandler.insertInEventTracker(user, LOG_PREFIX, logger);
        }
    }
}
