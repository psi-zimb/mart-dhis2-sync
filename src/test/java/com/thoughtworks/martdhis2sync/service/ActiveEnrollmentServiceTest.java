package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.step.NewActiveEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.NewActiveEnrollmentWithEventsStep;
import com.thoughtworks.martdhis2sync.step.UpdatedActiveEnrollmentWithEventsStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;

import java.io.SyncFailedException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ActiveEnrollmentServiceTest {
    @Mock
    private NewActiveEnrollmentWithEventsStep newActiveEnrollmentWithEventsStep;

    @Mock
    private UpdatedActiveEnrollmentWithEventsStep updatedActiveEnrollmentWithEventsStep;

    @Mock
    private NewActiveEnrollmentStep newActiveEnrollmentStep;
    @Mock
    private JobService jobService;

    @Mock
    private Logger logger;

    @Mock
    private Step step;

    private ActiveEnrollmentService service;

    private String programName = "HT Service";
    private String enrLookupTable = "enrollment_table";
    private String evnLookupTable = "event_table";
    private String user = "superman";
    private String mappingObj = "";
    private String jobName = "New Active Enrollments";
    private String updateJobName = "Updated Active Enrollments";
    private List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        service = new ActiveEnrollmentService();

        setValuesForMemberFields(service, "newEnrollmentWithEventsStep", newActiveEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "jobService", jobService);
        setValuesForMemberFields(service, "logger", logger);
        setValuesForMemberFields(service, "updatedEnrollmentWithEventsStep", updatedActiveEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "activeEnrollmentStep", newActiveEnrollmentStep);
    }

    @Test
    public void shouldTriggerTheJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newActiveEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(newActiveEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, jobName, steps);

        service.triggerJobForNewActiveEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj);

        verify(jobService, times(1)).triggerJob(programName, user, jobName, steps);
        verify(newActiveEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
        verify(newActiveEnrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFail() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newActiveEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(newActiveEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, jobName, steps);

        try {
            service.triggerJobForNewActiveEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj);
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, jobName, steps);
            verify(newActiveEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
            verify(logger, times(1)).error("Active Enrollments: Invalid Params");
        }
    }

    @Test
    public void shouldTriggerTheJobForUpdatedActiveEnrollments() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(updatedActiveEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore)).thenReturn(step);
        when(newActiveEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, updateJobName, steps);

        service.triggerJobForUpdatedActiveEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, enrollmentsToIgnore);

        verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps);
        verify(updatedActiveEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore);
        verify(newActiveEnrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFailForUpdatedActiveEnrollments() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(updatedActiveEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore)).thenReturn(step);
        when(newActiveEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, updateJobName, steps);

        try {
            service.triggerJobForUpdatedActiveEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, enrollmentsToIgnore);
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps);
            verify(updatedActiveEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore);
            verify(logger, times(1)).error("Active Enrollments: Invalid Params");
        }

    }
}