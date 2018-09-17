package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.listener.JobCompletionNotificationListener;
import com.thoughtworks.martdhis2sync.step.ProgramEnrollmentStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;

import java.util.Date;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ProgramEnrollmentService.class)
@PowerMockIgnore("javax.management.*")
public class ProgramEnrollmentServiceTest {

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
    private ProgramEnrollmentStep enrollmentStep;

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

    private ProgramEnrollmentService programEnrollmentService;

    @Before
    public void setUp() throws Exception {
        programEnrollmentService = new ProgramEnrollmentService();
        setValuesForMemberFields(programEnrollmentService, "jobLauncher", jobLauncher);
        setValuesForMemberFields(programEnrollmentService, "listener", listener);
        setValuesForMemberFields(programEnrollmentService, "jobBuilderFactory", jobBuilderFactory);
        setValuesForMemberFields(programEnrollmentService, "programEnrollmentStep", enrollmentStep);
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {
        String lookUpTable = "patient_program_enrollment";
        String user = "testUser";

        jobMocks(lookUpTable);
        jobParametersMocks();
        when(jobLauncher.run(job, jobParameters)).thenReturn(execution);

        programEnrollmentService.triggerJob(user, lookUpTable);

        verify(jobBuilderFactory, times(1)).get("syncProgramEnrollment");
        verifyNew(RunIdIncrementer.class, times(1)).withNoArguments();
        verify(jobBuilder, times(1)).incrementer(runIdIncrementer);
        verify(jobBuilder, times(1)).listener(listener);
        verify(enrollmentStep, times(1)).get(lookUpTable);
        verify(jobBuilder, times(1)).flow(step);
        verify(flowBuilder, times(1)).end();
        verify(flowJobBuilder, times(1)).build();

        verifyNew(JobParametersBuilder.class, times(1)).withNoArguments();
        verify(parametersBuilder, times(1)).addDate(anyString(), any(Date.class));
        verify(parametersBuilder, times(1)).toJobParameters();
        verify(jobLauncher, times(1)).run(job, jobParameters);
    }

    private void jobMocks(String lookUpTable) throws Exception {
        when(jobBuilderFactory.get("syncProgramEnrollment")).thenReturn(jobBuilder);
        whenNew(RunIdIncrementer.class).withNoArguments().thenReturn(runIdIncrementer);
        when(jobBuilder.incrementer(runIdIncrementer)).thenReturn(jobBuilder);
        when(jobBuilder.listener(listener)).thenReturn(jobBuilder);
        when(enrollmentStep.get(lookUpTable)).thenReturn(step);
        when(jobBuilder.flow(step)).thenReturn(flowBuilder);
        when(flowBuilder.end()).thenReturn(flowJobBuilder);
        when(flowJobBuilder.build()).thenReturn(job);
    }

    private void jobParametersMocks() throws Exception {
        whenNew(JobParametersBuilder.class).withNoArguments().thenReturn(parametersBuilder);
        when(parametersBuilder.addDate(anyString(), any(Date.class))).thenReturn(parametersBuilder);
        when(parametersBuilder.addString(anyString(), any(String.class))).thenReturn(parametersBuilder);
        when(parametersBuilder.toJobParameters()).thenReturn(jobParameters);
    }
}
