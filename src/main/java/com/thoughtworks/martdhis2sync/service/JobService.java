package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import lombok.Setter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.SyncFailedException;
import java.util.Date;
import java.util.List;

@Component
public class JobService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobCompletionNotificationListener listener;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private LoggerService loggerService;

    @Setter
    public static boolean IS_JOB_FAILED = false;

    public void triggerJob(String programName, String user, String jobName, List<Step> steps, String openLatestCompletedEnrollment)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        IS_JOB_FAILED = false;
        JobExecution jobExecution = jobLauncher.run(getJob(jobName, steps),
                new JobParametersBuilder()
                        .addDate("date", new Date())
                        .addString("service", programName)
                        .addString("user", user)
                        .addString("openLatestCompletedEnrollment", openLatestCompletedEnrollment)
                        .toJobParameters());

        if (jobExecution.getStatus() == BatchStatus.FAILED || IS_JOB_FAILED) {
            jobExecution.getAllFailureExceptions().forEach(exp -> {
                String message = exp.getMessage();
                if(message != null) {
                    loggerService.collateLogMessage(message);
                }
            });

            throw new SyncFailedException(jobName.toUpperCase() + " FAILED");
        }
    }

    private Job getJob(String jobName, List<Step> steps) {
        Step firstStep = steps.remove(0);
        FlowBuilder<FlowJobBuilder> flow = getFlow(jobName, firstStep);
        steps.forEach(flow::next);
        return flow.end().build();
    }

    private FlowBuilder<FlowJobBuilder> getFlow(String jobName, Step step) {
        return jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step);
    }
}
