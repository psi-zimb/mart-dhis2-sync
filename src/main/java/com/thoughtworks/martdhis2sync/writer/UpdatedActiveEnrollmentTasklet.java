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

import java.sql.SQLException;

@Component
public class UpdatedActiveEnrollmentTasklet implements Tasklet {

    @Autowired
    private TrackersHandler trackersHandler;


    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "ACTIVE ENROLLMENT TASKLET: ";

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String user = chunkContext.getStepContext().getJobParameters().get("user").toString();
        int recordsCreated;
        if (!EnrollmentUtil.enrollmentsToSaveInTracker.isEmpty()) {
            try {
                recordsCreated = trackersHandler.updateInEnrollmentTracker(user);
                logger.info(LOG_PREFIX + "Successfully updated " + recordsCreated + " Enrollment UIDs.");
            } catch (SQLException e) {
                logger.error(LOG_PREFIX + "Exception occurred while inserting Program Enrollment UIDs:" + e.getMessage());
                e.printStackTrace();
            }
        }
        if (!EventUtil.eventsToSaveInTracker.isEmpty()) {
            try {
                recordsCreated = trackersHandler.insertInEventTracker(user);
                logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " Event UIDs.");
            } catch (SQLException e) {
                logger.error(LOG_PREFIX + "Exception occurred while inserting Event UIDs:" + e.getMessage());
                e.printStackTrace();
            }
        }
        return RepeatStatus.FINISHED;
    }
}
