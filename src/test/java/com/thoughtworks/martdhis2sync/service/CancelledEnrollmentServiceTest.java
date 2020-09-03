package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.step.NewCancelledEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.NewCancelledEnrollmentWithEventsStep;
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
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class CancelledEnrollmentServiceTest {

    @Mock
    private NewCancelledEnrollmentWithEventsStep newEnrollmentWithEventsStep;

    @Mock
    private NewCancelledEnrollmentStep CancelledEnrollmentStep;

    @Mock
    private JobService jobService;

    @Mock
    private Logger logger;

    @Mock
    private Step step;

//    @Mock
//    private UpdatedCancelledEnrollmentWithEventsStep updatedCancelledEnrollmentWithEventsStep;
//
//    @Mock
//    private UpdatedCancelledEnrollmentStep updatedCancelledEnrollmentStep;

    private CancelledEnrollmentService service;

    private String programName = "HT Service";
    private String enrLookupTable = "enrollment_table";
    private String evnLookupTable = "event_table";
    private String user = "superman";
    private String mappingObj = "";
    private String jobName = "New Cancelled Enrollments";
    private String updateJobName = "Updated Cancelled Enrollments";
    private List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();
    private String openLatestCancelledEnrollment = "no";

    @Before
    public void setUp() throws Exception {
        service = new CancelledEnrollmentService();

        setValuesForMemberFields(service, "newEnrollmentWithEventsStep", newEnrollmentWithEventsStep);
        setValuesForMemberFields(service, "cancelledEnrollmentStep", CancelledEnrollmentStep);
        setValuesForMemberFields(service, "jobService", jobService);
        setValuesForMemberFields(service, "logger", logger);
//        setValuesForMemberFields(service, "updatedEnrollmentWithEventsStep", updatedCancelledEnrollmentWithEventsStep);
//        setValuesForMemberFields(service, "updatedCancelledEnrollmentStep", updatedCancelledEnrollmentStep);
    }

    @Test
    public void shouldTriggerTheJob() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(CancelledEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doNothing().when(jobService).triggerJob(programName, user, jobName, steps, openLatestCancelledEnrollment);

        service.triggerJobForNewCancelledEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, openLatestCancelledEnrollment);

        verify(jobService, times(1)).triggerJob(programName, user, jobName, steps, openLatestCancelledEnrollment);
        verify(newEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
        verify(CancelledEnrollmentStep, times(1)).get();
    }

    @Test
    public void shouldLogErrorOnJobFail() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
        when(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj)).thenReturn(step);
        when(CancelledEnrollmentStep.get()).thenReturn(step);
        LinkedList<Step> steps = new LinkedList<>();
        steps.add(step);
        steps.add(step);
        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, jobName, steps, openLatestCancelledEnrollment);

        try {
            service.triggerJobForNewCancelledEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, openLatestCancelledEnrollment);
        } catch (Exception e) {
            verify(jobService, times(1)).triggerJob(programName, user, jobName, steps, openLatestCancelledEnrollment);
            verify(newEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj);
            verify(CancelledEnrollmentStep, times(1)).get();
            verify(logger, times(1)).error("Cancelled Enrollments: Invalid Params");
        }

    }

//    @Test
//    public void shouldTriggerTheJobForUpdatedCancelledEnrollments() throws JobParametersInvalidException,
//            JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
//        when(updatedCancelledEnrollmentWithEventsStep
//                .get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore))
//                .thenReturn(step);
//        when(updatedCancelledEnrollmentStep.get()).thenReturn(step);
//        LinkedList<Step> steps = new LinkedList<>();
//        steps.add(step);
//        steps.add(step);
//        doNothing().when(jobService).triggerJob(programName, user, updateJobName, steps, openLatestCancelledEnrollment);
//
//        service.triggerJobForUpdatedCancelledEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, enrollmentsToIgnore, openLatestCancelledEnrollment);
//
//        verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps, openLatestCancelledEnrollment);
//        verify(updatedCancelledEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore);
//        verify(updatedCancelledEnrollmentStep, times(1)).get();
//    }

//    @Test
//    public void shouldLogErrorOnJobFailForUpdateCancelledEnrollments() throws JobParametersInvalidException,
//            JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {
//        when(updatedCancelledEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore)).thenReturn(step);
//        when(updatedCancelledEnrollmentStep.get()).thenReturn(step);
//        LinkedList<Step> steps = new LinkedList<>();
//        steps.add(step);
//        steps.add(step);
//        doThrow(new JobParametersInvalidException("Invalid Params")).when(jobService).triggerJob(programName, user, updateJobName, steps, openLatestCancelledEnrollment);
//
//        try {
//            service.triggerJobForUpdatedCancelledEnrollments(programName, user, enrLookupTable, evnLookupTable, mappingObj, enrollmentsToIgnore, openLatestCancelledEnrollment);
//        } catch (Exception e) {
//            verify(jobService, times(1)).triggerJob(programName, user, updateJobName, steps, openLatestCancelledEnrollment);
//            verify(updatedCancelledEnrollmentWithEventsStep, times(1)).get(enrLookupTable, evnLookupTable, programName, mappingObj, enrollmentsToIgnore);
//            verify(updatedCancelledEnrollmentStep, times(1)).get();
//            verify(logger, times(1)).error("Cancelled Enrollments: Invalid Params");
//        }
//
//    }
}
