package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.ProgramEnrollmentStep;
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
public class ProgramEnrollmentService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobCompletionNotificationListener listener;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private ProgramEnrollmentStep programEnrollmentStep;

    private Logger logger = LoggerFactory.getLogger(TEIService.class);

    private static final String LOG_PREFIX = "Program Enrollment Service: ";

    public void triggerJob(String user, String lookupTable) throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException {

        try {
            jobLauncher.run(syncProgramEnrollmentJob(lookupTable),
                    new JobParametersBuilder()
                            .addDate("date", new Date())
                            .addString("user", user)
                            .toJobParameters());
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }

    private Job syncProgramEnrollmentJob(String lookupTable) {
        return jobBuilderFactory.get("syncProgramEnrollment")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(programEnrollmentStep.get(lookupTable))
                .end()
                .build();
    }
}
