package com.thoughtworks.martdhis2sync.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    public JobCompletionNotificationListener() {
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("Job# " + jobExecution.getJobId()
                + " triggered by " + jobExecution.getJobParameters().getString("user") + " "
                + jobExecution.getStatus() + " and took "
                + (jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime()) + " milliseconds.");
    }
}
