package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.SyncFailedException;

@Component
public class TEIService {

    @Autowired
    private TrackedEntityInstanceStep trackedEntityInstanceStep;

    @Autowired
    private JobService jobService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "TEI Service: ";
    private static final String TEI_JOB_NAME = "Sync Tracked Entity Instance";

    public void triggerJob(String service, String user, String lookupTable, Object mappingObj)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        try {
            jobService.triggerJob(service, user, lookupTable, TEI_JOB_NAME, trackedEntityInstanceStep, mappingObj);
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }
}
