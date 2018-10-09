package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.StepBuilderContract;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
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

import java.io.SyncFailedException;
import java.util.Date;

@Component
public class JobService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobCompletionNotificationListener listener;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    public void triggerJob(String programName, String user, String lookupTable, String jobName, StepBuilderContract step, Object mappingObj)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        JobExecution jobExecution = jobLauncher.run(getJob(lookupTable, programName, jobName, step, mappingObj),
                new JobParametersBuilder()
                        .addDate("date", new Date())
                        .addString("service", programName)
                        .addString("user", user)
                        .toJobParameters());

        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            jobExecution.getAllFailureExceptions().forEach(exp -> {
                String message = exp.getMessage();
                if(message != null) {
                    PushController.statusInfo.append(message).append(", ");
                };
            });

            throw new SyncFailedException(jobName.toUpperCase() + " FAILED");
        }
    }

    private Job getJob(String lookupTable, String programName, String jobName, StepBuilderContract step, Object mappingObj) {
        return jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step.get(lookupTable, programName, mappingObj))
                .end()
                .build();
    }
}
