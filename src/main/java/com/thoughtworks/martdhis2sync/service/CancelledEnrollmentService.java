package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.step.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.SyncFailedException;
import java.util.LinkedList;
import java.util.List;

@Component
public class CancelledEnrollmentService {

    @Autowired
    private NewCancelledEnrollmentWithEventsStep newEnrollmentWithEventsStep;

    @Autowired
    private NewCancelledEnrollmentStep cancelledEnrollmentStep;

    @Autowired
    private UpdatedCancelledEnrollmentWithEventsStep updatedCancelledEnrollmentWithEventsStep;

    @Autowired
    private UpdatedCancelledEnrollmentStep updatedCancelledEnrollmentStep;

    @Autowired
    private JobService jobService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "Cancelled Enrollments: ";
    private static final String JOB_NEW_CANCELLED_ENROLLMENTS = "New Cancelled Enrollments";
    private static final String JOB_UPDATED_CANCELLED_ENROLLMENTS = "Updated Cancelled Enrollments";



    public void triggerJobForNewCancelledEnrollments(String service, String user, String insLookupTable,String enrLookupTable,
                                                     String evnLookupTable, Object mappingObj, String openLatestCompletedEnrollment, String startDate, String endDate)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        LinkedList<Step> steps = new LinkedList<>();
        steps.add(newEnrollmentWithEventsStep.get(insLookupTable,enrLookupTable, evnLookupTable, service, mappingObj,startDate, endDate));
        steps.add(cancelledEnrollmentStep.get());
        triggerJob(service, user, steps, JOB_NEW_CANCELLED_ENROLLMENTS, openLatestCompletedEnrollment);
    }

    public void triggerJobForUpdatedCancelledEnrollments(String service, String user, String enrLookupTable,
                                                         String evnLookupTable, Object mappingObj, List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String openLatestCompletedEnrollment, String startDate, String endDate)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        LinkedList<Step> steps = new LinkedList<>();
        steps.add(updatedCancelledEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, service, mappingObj, enrollmentsToIgnore, startDate, endDate));
        steps.add(updatedCancelledEnrollmentStep.get());
        triggerJob(service, user, steps, JOB_UPDATED_CANCELLED_ENROLLMENTS, openLatestCompletedEnrollment);
    }

    private void triggerJob(String service, String user, LinkedList<Step> steps, String jobName, String openLatestCompletedEnrollment)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException,
            JobInstanceAlreadyCompleteException, SyncFailedException {
        try {
            jobService.triggerJob(service, user, jobName, steps, openLatestCompletedEnrollment);
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }
}
