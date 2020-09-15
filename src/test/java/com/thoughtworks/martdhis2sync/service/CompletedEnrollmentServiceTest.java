package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.step.NewCompletedEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.NewCompletedEnrollmentWithEventsStep;
import com.thoughtworks.martdhis2sync.step.UpdatedCompletedEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.UpdatedCompletedEnrollmentWithEventsStep;
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
public class CompletedEnrollmentServiceTest {
    @Mock
    private NewCompletedEnrollmentWithEventsStep newEnrollmentWithEventsStep;

    @Mock
    private NewCompletedEnrollmentStep completedEnrollmentStep;

    @Mock
    private JobService jobService;

    @Mock
    private Logger logger;

    @Mock
    private Step step;

    @Mock
    private UpdatedCompletedEnrollmentWithEventsStep updatedCompletedEnrollmentWithEventsStep;

    @Mock
    private UpdatedCompletedEnrollmentStep updatedCompletedEnrollmentStep;

    private CompletedEnrollmentService service;

    private String programName = "HT Service";
    private String enrLookupTable = "enrollment_table";
    private String evnLookupTable = "event_table";
    private String user = "superman";
    private String mappingObj = "";
    private String jobName = "New Completed Enrollments";
    private String updateJobName = "Updated Completed Enrollments";
    private List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();
    private String openLatestCompletedEnrollment = "no";

    @Before
    public void setUp() throws Exception {
        service = new CompletedEnrollmentService();

        setValuesForMemberFields(service, "newEnrollmentWithEventsStep", newEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "completedEnrollmentStep", completedEnrollmentStep);
        setValuesForMemberFields(service, "jobService", jobService);
        setValuesForMemberFields(service, "logger", logger);
        setValuesForMemberFields(service, "updatedEnrollmentWithEventsStep", updatedCompletedEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "updatedCompletedEnrollmentStep", updatedCompletedEnrollmentStep);
    }

    @Test
    public void shouldTriggerTheJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj,"","")).thenReturn(step);
        when(completedEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);

        service.triggerJobForNewCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, openLatestCompletedEnrollment,"","");

        verify(jobService, times(1)).triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);
        verify(newEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj,"","");
        verify(completedEnrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFail() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj, "", "")).thenReturn(step);
        when(completedEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);

        try {
            service.triggerJobForNewCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, openLatestCompletedEnrollment,"","");
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, jobName, steps, openLatestCompletedEnrollment);
            verify(newEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj,"","");
            verify(completedEnrollmentStep, times(1)).get();
            verify(logger, times(1)).error("Completed Enrollments: Invalid Params");
        }

    }

    @Test
    public void shouldTriggerTheJobForUpdatedCompletedEnrollments() throws JobParametersInvalidException,
            JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(updatedCompletedEnrollmentWithEventsStep
                .get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore, "", ""))
                .thenReturn(step);
        when(updatedCompletedEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, updateJobName, steps, openLatestCompletedEnrollment);

        service.triggerJobForUpdatedCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, enrollmentsToIgnore, openLatestCompletedEnrollment, "", "");

        verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps, openLatestCompletedEnrollment);
        verify(updatedCompletedEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore, "", "");
        verify(updatedCompletedEnrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFailForUpdateCompletedEnrollments() throws JobParametersInvalidException,
            JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(updatedCompletedEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore, "", "")).thenReturn(step);
        when(updatedCompletedEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, updateJobName, steps, openLatestCompletedEnrollment);

        try {
            service.triggerJobForUpdatedCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, enrollmentsToIgnore, openLatestCompletedEnrollment, "","");
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps, openLatestCompletedEnrollment);
            verify(updatedCompletedEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore,"","");
            verify(updatedCompletedEnrollmentStep, times(1)).get();
            verify(logger, times(1)).error("Completed Enrollments: Invalid Params");
        }

    }
}
