package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.step.NewCompletedEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.NewCompletedEnrollmentWithEventsStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.SyncFailedException;
import java.util.LinkedList;

@Component
public class CompletedEnrollmentService {

    @Autowired
    private NewCompletedEnrollmentWithEventsStep enrollmentWithEventsStep;

    @Autowired
    private NewCompletedEnrollmentStep enrollmentStep;

    @Autowired
    private JobService jobService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "Completed Enrollments: ";
    private static final String JOB_NAME = "New Completed Enrollments";

    public void triggerJob(String service, String user, String enrLookupTable, String envLookupTable, Object mappingObj)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        try {
            LinkedList<Step> steps = new LinkedList<>();
            steps.add(enrollmentWithEventsStep.get(enrLookupTable, envLookupTable, service, mappingObj));
            steps.add(enrollmentStep.get());
            jobService.triggerJob(service, user, JOB_NAME, steps);
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }
}
