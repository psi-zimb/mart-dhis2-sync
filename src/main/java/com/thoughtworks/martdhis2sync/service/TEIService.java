package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TEIService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobCompletionNotificationListener listener;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private TrackedEntityInstanceStep trackedEntityInstanceStep;

    private Logger logger = LoggerFactory.getLogger(TEIService.class);

    private static final String LOG_PREFIX = "TEI Service: ";

    public void triggerJob(String service, String user, String lookupTable, Object mappingObj)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException {

        try {
            jobLauncher.run(syncTrackedEntityInstanceJob(lookupTable, mappingObj),
                    new JobParametersBuilder()
                            .addDate("date", new Date())
                            .addString("service", service)
                            .addString("user", user)
                            .toJobParameters());
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }

    private Job syncTrackedEntityInstanceJob(String lookupTable, Object mappingObj) {
        return jobBuilderFactory.get("syncTrackedEntityInstance")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(trackedEntityInstanceStep.get(lookupTable, mappingObj))
                .end()
                .build();
    }
}
