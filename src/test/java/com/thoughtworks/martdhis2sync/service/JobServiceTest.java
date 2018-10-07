package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;

import java.io.SyncFailedException;
import java.util.Collections;
import java.util.Date;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobService.class)
@PowerMockIgnore("javax.management.*")
public class JobServiceTest {

    @Mock
    private JobLauncher jobLauncher;

    @Mock
    private JobBuilderFactory jobBuilderFactory;

    @Mock
    private JobBuilder jobBuilder;

    @Mock
    private RunIdIncrementer runIdIncrementer;

    @Mock
    private JobCompletionNotificationListener listener;

    @Mock
    private TrackedEntityInstanceStep instanceStep;

    @Mock
    private Step step;

    @Mock
    private JobFlowBuilder flowBuilder;

    @Mock
    private FlowJobBuilder flowJobBuilder;

    @Mock
    private Job job;

    @Mock
    private JobParametersBuilder parametersBuilder;

    @Mock
    private JobParameters jobParameters;

    @Mock
    private JobExecution execution;

    @Mock
    private Throwable throwable;

    private JobService jobService;

    private String lookUpTable = "patient_identifier";
    private Object mappingObj = "";
    private String programName = "serviceName";
    private String user = "Admin";
    private String jobName = "syncTrackedEntityInstance";

    @Before
    public void setUp() throws Exception {
        jobService = new JobService();
        setValuesForMemberFields(jobService, "jobLauncher", jobLauncher);
        setValuesForMemberFields(jobService, "listener", listener);
        setValuesForMemberFields(jobService, "jobBuilderFactory", jobBuilderFactory);
        PushController.failedReason = new StringBuilder();
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {

        jobMocks();
        jobParametersMocks();
        when(jobLauncher.run(job, jobParameters)).thenReturn(execution);

        jobService.triggerJob(programName, user, lookUpTable, jobName, instanceStep, mappingObj);

        verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
        verifyNew(RunIdIncrementer.class, times(1)).withNoArguments();
        verify(jobBuilder, times(1)).incrementer(runIdIncrementer);
        verify(jobBuilder, times(1)).listener(listener);
        verify(instanceStep, times(1)).get(lookUpTable, programName, mappingObj);
        verify(jobBuilder, times(1)).flow(step);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();

        verifyNew(JobParametersBuilder.class, times(1)).withNoArguments();
        verify(parametersBuilder, times(1)).addDate(anyString(), any(Date.class));
        verify(parametersBuilder, times(1)).addString("service", programName);
        verify(parametersBuilder, times(1)).toJobParameters();
        verify(jobLauncher, times(1)).run(job, jobParameters);
    }

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void shouldThrowJobExecutionAlreadyRunningException() throws Exception {

        jobMocks();
        jobParametersMocks();
        when(jobLauncher.run(job, jobParameters))
                .thenThrow(new JobExecutionAlreadyRunningException("Job Execution Already Running"));

        jobService.triggerJob(programName, user, lookUpTable, jobName, instanceStep, mappingObj);
    }

    @Test(expected = SyncFailedException.class)
    public void shouldThrowSyncFailedException() throws Exception {
        jobMocks();
        jobParametersMocks();
        when(jobLauncher.run(job, jobParameters)).thenReturn(execution);
        when(execution.getStatus()).thenReturn(BatchStatus.FAILED);

        jobService.triggerJob(programName, user, lookUpTable, jobName, instanceStep, mappingObj);

        verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
        verifyNew(RunIdIncrementer.class, times(1)).withNoArguments();
        verify(jobBuilder, times(1)).incrementer(runIdIncrementer);
        verify(jobBuilder, times(1)).listener(listener);
        verify(instanceStep, times(1)).get(lookUpTable, programName, mappingObj);
        verify(jobBuilder, times(1)).flow(step);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();

        verifyNew(JobParametersBuilder.class, times(1)).withNoArguments();
        verify(parametersBuilder, times(1)).addDate(anyString(), any(Date.class));
        verify(parametersBuilder, times(1)).addString("service", programName);
        verify(parametersBuilder, times(1)).toJobParameters();
        verify(jobLauncher, times(1)).run(job, jobParameters);
        verify(execution, times(1)).getStatus();
    }

    @Test
    public void shouldUpdateFailedReasonWhenExceptionIsThere() throws Exception {
        jobMocks();
        jobParametersMocks();
        String expMessage = "Failed to Initialize the Reader";

        when(jobLauncher.run(job, jobParameters)).thenReturn(execution);
        when(execution.getStatus()).thenReturn(BatchStatus.FAILED);
        when(execution.getAllFailureExceptions()).thenReturn(Collections.singletonList(throwable));
        when(throwable.getMessage()).thenReturn(expMessage);

        try {
            jobService.triggerJob(programName, user, lookUpTable, jobName, instanceStep, mappingObj);
        } catch (SyncFailedException e) {
            verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
            verifyNew(RunIdIncrementer.class, times(1)).withNoArguments();
            verify(jobBuilder, times(1)).incrementer(runIdIncrementer);
            verify(jobBuilder, times(1)).listener(listener);
            verify(instanceStep, times(1)).get(lookUpTable, programName, mappingObj);
            verify(jobBuilder, times(1)).flow(step);
            verify(flowBuilder, times(1)).end();
            verify(flowJobBuilder, times(1)).build();

            verifyNew(JobParametersBuilder.class, times(1)).withNoArguments();
            verify(parametersBuilder, times(1)).addDate(anyString(), any(Date.class));
            verify(parametersBuilder, times(1)).addString("service", programName);
            verify(parametersBuilder, times(1)).toJobParameters();
            verify(jobLauncher, times(1)).run(job, jobParameters);
            verify(execution, times(1)).getStatus();
            verify(execution, times(1)).getAllFailureExceptions();
            verify(throwable, times(1)).getMessage();

            assertEquals(expMessage + ", ", PushController.failedReason.toString());
        }
    }

    private void jobMocks() throws Exception {
        when(jobBuilderFactory.get(jobName)).thenReturn(jobBuilder);
        whenNew(RunIdIncrementer.class).withNoArguments().thenReturn(runIdIncrementer);
        when(jobBuilder.incrementer(runIdIncrementer)).thenReturn(jobBuilder);
        when(jobBuilder.listener(listener)).thenReturn(jobBuilder);
        when(instanceStep.get(lookUpTable, programName, mappingObj)).thenReturn(step);
        when(jobBuilder.flow(step)).thenReturn(flowBuilder);
        when(flowBuilder.end()).thenReturn(flowJobBuilder);
        when(flowJobBuilder.build()).thenReturn(job);
    }

    private void jobParametersMocks() throws Exception {
        whenNew(JobParametersBuilder.class).withNoArguments().thenReturn(parametersBuilder);
        when(parametersBuilder.addDate(anyString(), any(Date.class))).thenReturn(parametersBuilder);
        when(parametersBuilder.addString("service", programName)).thenReturn(parametersBuilder);
        when(parametersBuilder.addString("user", user)).thenReturn(parametersBuilder);
        when(parametersBuilder.toJobParameters()).thenReturn(jobParameters);
    }
}