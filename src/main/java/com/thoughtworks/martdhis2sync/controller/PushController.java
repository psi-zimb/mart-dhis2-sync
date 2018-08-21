package com.thoughtworks.martdhis2sync.controller;

import com.thoughtworks.martdhis2sync.config.AppConfig;
import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PushController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private AppConfig batchConfig;

    @Autowired
    private JobCompletionNotificationListener listener;

    @GetMapping(value = "/pushData")
    public Map<String, String> pushData()
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException {

        Map<String, String> result = new HashMap<>();

        jobLauncher.run(batchConfig.syncTrackedEntityInstanceJob(listener),
                new JobParametersBuilder().addDate("run.date", new Date()).toJobParameters());

        result.put("Job Status", "Executed");
        return result;
    }
}
