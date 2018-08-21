package com.thoughtworks.martdhis2sync.config;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@EnableBatchProcessing
@Configuration
public class AppConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private TrackedEntityInstanceStep trackedEntityInstanceStep;


    public Job syncTrackedEntityInstanceJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("syncTrackedEntityInstance")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(trackedEntityInstanceStep.getStep())
                .end()
                .build();
    }
}
