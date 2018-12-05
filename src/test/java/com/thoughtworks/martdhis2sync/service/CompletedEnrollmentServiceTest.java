package com.thoughtworks.martdhis2sync.service;

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
import java.util.LinkedList;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class CompletedEnrollmentServiceTest {
    @Mock
    private NewCompletedEnrollmentWithEventsStep newEnrollmentWithEventsStep;

    @Mock
    private NewCompletedEnrollmentStep enrollmentStep;

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

    @Before
    public void setUp() throws Exception {
        service = new CompletedEnrollmentService();

        setValuesForMemberFields(service, "newEnrollmentWithEventsStep", newEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "enrollmentStep", enrollmentStep);
        setValuesForMemberFields(service, "jobService", jobService);
        setValuesForMemberFields(service, "logger", logger);
        setValuesForMemberFields(service, "updatedEnrollmentWithEventsStep", updatedCompletedEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "updatedCompletedEnrollmentStep", updatedCompletedEnrollmentStep);
    }

    @Test
    public void shouldTriggerTheJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(enrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, jobName, steps);

        service.triggerJobForNewCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj);

        verify(jobService, times(1)).triggerJob(programName, user, jobName, steps);
        verify(newEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
        verify(enrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFail() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(enrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, jobName, steps);

        try {
            service.triggerJobForNewCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj);
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, jobName, steps);
            verify(newEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
            verify(enrollmentStep, times(1)).get();
            verify(logger, times(1)).error("Completed Enrollments: Invalid Params");
        }

    }

    @Test
    public void shouldTriggerTheJobForUpdatedCompletedEnrollments() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(updatedCompletedEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(updatedCompletedEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, updateJobName, steps);

        service.triggerJobForUpdatedCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj);

        verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps);
        verify(updatedCompletedEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
        verify(updatedCompletedEnrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFailForUpdateCompletedEnrollments() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(updatedCompletedEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(updatedCompletedEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, updateJobName, steps);

        try {
            service.triggerJobForUpdatedCompletedEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj);
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps);
            verify(updatedCompletedEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
            verify(updatedCompletedEnrollmentStep, times(1)).get();
            verify(logger, times(1)).error("Completed Enrollments: Invalid Params");
        }

    }
}
