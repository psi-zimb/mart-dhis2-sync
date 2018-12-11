package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;

import java.io.SyncFailedException;
import java.util.Collections;
import java.util.LinkedList;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class JobServiceTest {

    @Mock
    private JobLauncher jobLauncher;

    @Mock
    private JobBuilderFactory jobBuilderFactory;

    @Mock
    private JobBuilder jobBuilder;

    @Mock
    private JobCompletionNotificationListener listener;

    @Mock
    private Step step1;

    @Mock
    private Step step2;

    @Mock
    private JobFlowBuilder flowBuilder;

    @Mock
    private FlowJobBuilder flowJobBuilder;

    @Mock
    private Job job;

    @Mock
    private JobExecution execution;

    @Mock
    private Throwable throwable;

    @Mock
    private LoggerService loggerService;

    private JobService jobService;

    private String programName = "serviceName";
    private String user = "Admin";
    private String jobName = "syncTrackedEntityInstance";
    private LinkedList<Step> steps = new LinkedList<>();
    private String openLatestCompletedEnrollment = "no";

    @Before
    public void setUp() throws Exception {
        jobService = new JobService();
        setValuesForMemberFields(jobService, "jobLauncher", jobLauncher);
        setValuesForMemberFields(jobService, "listener", listener);
        setValuesForMemberFields(jobService, "jobBuilderFactory", jobBuilderFactory);
        setValuesForMemberFields(jobService, "loggerService", loggerService);
        steps.add(step1);
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {

        jobMocks();
        when(jobLauncher.run(any(Job.class), any(JobParameters.class))).thenReturn(execution);

        jobService.triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);

        verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
        verify(jobBuilder, times(1)).incrementer(any());
        verify(jobBuilder, times(1)).listener(listener);
        verify(jobBuilder, times(1)).flow(step1);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();
    }

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void shouldThrowJobExecutionAlreadyRunningException() throws Exception {

        jobMocks();
        when(jobLauncher.run(any(Job.class), any(JobParameters.class)))
                .thenThrow(new JobExecutionAlreadyRunningException("Job Execution Already Running"));

        jobService.triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);
    }

    @Test(expected = SyncFailedException.class)
    public void shouldThrowSyncFailedException() throws Exception {
        jobMocks();
        when(jobLauncher.run(any(Job.class), any(JobParameters.class))).thenReturn(execution);
        when(execution.getStatus()).thenReturn(BatchStatus.FAILED);

        jobService.triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);

        verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
        verify(jobBuilder, times(1)).incrementer(any());
        verify(jobBuilder, times(1)).listener(listener);
        verify(jobBuilder, times(1)).flow(step1);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();
    }

    @Test
    public void shouldUpdateStatusInfoWhenExceptionIsThere() throws Exception {
        jobMocks();
        String expMessage = "Failed to Initialize the Reader";

        when(jobLauncher.run(any(Job.class), any(JobParameters.class))).thenReturn(execution);
        when(execution.getStatus()).thenReturn(BatchStatus.FAILED);
        when(execution.getAllFailureExceptions()).thenReturn(Collections.singletonList(throwable));
        when(throwable.getMessage()).thenReturn(expMessage);

        try {
            jobService.triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);
        } catch (SyncFailedException e) {
            verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
            verify(jobBuilder, times(1)).incrementer(any());
            verify(jobBuilder, times(1)).listener(listener);
            verify(jobBuilder, times(1)).flow(step1);
            verify(flowBuilder, times(1)).end();
            verify(flowJobBuilder, times(1)).build();

            verify(jobLauncher, times(1)).run(any(Job.class), any(JobParameters.class));
            verify(execution, times(1)).getStatus();
            verify(execution, times(1)).getAllFailureExceptions();
            verify(throwable, times(1)).getMessage();

            verify(loggerService, times(1)).collateLogMessage(expMessage);
        }
    }

    @Test
    public void shouldNotCallLoggerServiceWhenThereIsNoMessage() throws Exception {
        jobMocks();

        when(jobLauncher.run(any(Job.class), any(JobParameters.class))).thenReturn(execution);
        when(execution.getStatus()).thenReturn(BatchStatus.FAILED);
        when(execution.getAllFailureExceptions()).thenReturn(Collections.singletonList(throwable));
        when(throwable.getMessage()).thenReturn(null);

        try {
            jobService.triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);
        } catch (SyncFailedException e) {
            verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
            verify(jobBuilder, times(1)).incrementer(any());
            verify(jobBuilder, times(1)).listener(listener);
            verify(jobBuilder, times(1)).flow(step1);
            verify(flowBuilder, times(1)).end();
            verify(flowJobBuilder, times(1)).build();

            verify(jobLauncher, times(1)).run(any(Job.class), any(JobParameters.class));
            verify(execution, times(1)).getStatus();
            verify(execution, times(1)).getAllFailureExceptions();
            verify(throwable, times(1)).getMessage();

            verify(loggerService, times(0)).collateLogMessage(null);
        }
    }

    @Test
    public void shouldTriggerTheJobWithMultipleSteps() throws Exception {
        steps.add(step2);
        jobMocks();
        when(jobLauncher.run(any(Job.class), any(JobParameters.class))).thenReturn(execution);

        jobService.triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);

        verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
        verify(jobBuilder, times(1)).incrementer(any());
        verify(jobBuilder, times(1)).listener(listener);
        verify(jobBuilder, times(1)).flow(step1);
        verify(flowBuilder, times(1)).next(step2);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();
    }

    private void jobMocks() throws Exception {
        when(jobBuilderFactory.get(jobName)).thenReturn(jobBuilder);
        when(jobBuilder.incrementer(any())).thenReturn(jobBuilder);
        when(jobBuilder.listener(listener)).thenReturn(jobBuilder);
        when(jobBuilder.flow(step1)).thenReturn(flowBuilder);
        when(flowBuilder.next(step2)).thenReturn(flowBuilder);
        when(flowBuilder.end()).thenReturn(flowJobBuilder);
        when(flowJobBuilder.build()).thenReturn(job);
    }
}
