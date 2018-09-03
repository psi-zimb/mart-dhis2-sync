package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
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

import java.util.Date;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TEIService.class)
public class TEIServiceTest {

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

    private TEIService teiService;

    private Date syncedDate = new Date();

    @Before
    public void setUp() throws Exception {
        teiService = new TEIService();
        setValuesForMemberFields(teiService, "jobLauncher", jobLauncher);
        setValuesForMemberFields(teiService, "listener", listener);
        setValuesForMemberFields(teiService, "jobBuilderFactory", jobBuilderFactory);
        setValuesForMemberFields(teiService, "trackedEntityInstanceStep", instanceStep);
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";

        jobMocks(lookUpTable, mappingObj, service);
        jobParametersMocks(service, user);
        when(jobLauncher.run(job, jobParameters)).thenReturn(execution);
        whenNew(Date.class).withNoArguments().thenReturn(syncedDate);

        teiService.triggerJob(service, user, lookUpTable, mappingObj);

        verify(jobBuilderFactory, times(1)).get("syncTrackedEntityInstance");
        verifyNew(RunIdIncrementer.class, times(1)).withNoArguments();
        verify(jobBuilder, times(1)).incrementer(runIdIncrementer);
        verify(jobBuilder, times(1)).listener(listener);
        verify(instanceStep, times(1)).get(lookUpTable, mappingObj, service, syncedDate);
        verify(jobBuilder, times(1)).flow(step);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();

        verifyNew(JobParametersBuilder.class, times(1)).withNoArguments();
        verify(parametersBuilder, times(1)).addDate(anyString(), any(Date.class));
        verify(parametersBuilder, times(1)).addString("service", service);
        verify(parametersBuilder, times(1)).toJobParameters();
        verify(jobLauncher, times(1)).run(job, jobParameters);
    }

    @Test (expected = JobExecutionAlreadyRunningException.class)
    public void shouldThrowJobExecutionAlreadyRunningException() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";

        jobMocks(lookUpTable, mappingObj, service);
        jobParametersMocks(service, user);
        when(jobLauncher.run(job, jobParameters))
                .thenThrow(new JobExecutionAlreadyRunningException("Job Execution Already Running"));
        whenNew(Date.class).withNoArguments().thenReturn(syncedDate);

        teiService.triggerJob(service, user, lookUpTable, mappingObj);
    }

    private void jobMocks(String lookUpTable, Object mappingObj, String service) throws Exception {
        when(jobBuilderFactory.get("syncTrackedEntityInstance")).thenReturn(jobBuilder);
        whenNew(RunIdIncrementer.class).withNoArguments().thenReturn(runIdIncrementer);
        when(jobBuilder.incrementer(runIdIncrementer)).thenReturn(jobBuilder);
        when(jobBuilder.listener(listener)).thenReturn(jobBuilder);
        when(instanceStep.get(lookUpTable, mappingObj, service, syncedDate)).thenReturn(step);
        when(jobBuilder.flow(step)).thenReturn(flowBuilder);
        when(flowBuilder.end()).thenReturn(flowJobBuilder);
        when(flowJobBuilder.build()).thenReturn(job);
    }

    private void jobParametersMocks(String service, String user) throws Exception {
        whenNew(JobParametersBuilder.class).withNoArguments().thenReturn(parametersBuilder);
        when(parametersBuilder.addDate(anyString(), any(Date.class))).thenReturn(parametersBuilder);
        when(parametersBuilder.addString("service", service)).thenReturn(parametersBuilder);
        when(parametersBuilder.addString("user", user)).thenReturn(parametersBuilder);
        when(parametersBuilder.toJobParameters()).thenReturn(jobParameters);
    }
}