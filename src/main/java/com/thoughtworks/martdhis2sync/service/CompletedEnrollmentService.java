package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.step.NewCompletedEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.NewCompletedEnrollmentWithEventsStep;
import com.thoughtworks.martdhis2sync.step.UpdatedCompletedEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.UpdatedCompletedEnrollmentWithEventsStep;
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
public class CompletedEnrollmentService {

    @Autowired
    private NewCompletedEnrollmentWithEventsStep newEnrollmentWithEventsStep;

    @Autowired
    private NewCompletedEnrollmentStep completedEnrollmentStep;

    @Autowired
    private UpdatedCompletedEnrollmentWithEventsStep updatedEnrollmentWithEventsStep;

    @Autowired
    private UpdatedCompletedEnrollmentStep updatedCompletedEnrollmentStep;

    @Autowired
    private JobService jobService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "Completed Enrollments: ";
    private static final String JOB_NEW_COMPLETED_ENROLLMENTS = "New Completed Enrollments";
    private static final String JOB_UPDATED_COMPLETED_ENROLLMENTS = "Updated Completed Enrollments";



    public void triggerJobForNewCompletedEnrollments(String service, String user, String insLookupTable,String enrLookupTable,
                                                     String evnLookupTable, Object mappingObj, String openLatestCompletedEnrollment, String startDate, String endDate)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        LinkedList<Step> steps = new LinkedList<>();

        steps.add(newEnrollmentWithEventsStep.get(insLookupTable,enrLookupTable, evnLookupTable, service, mappingObj, startDate, endDate));
        steps.add(completedEnrollmentStep.get());
        triggerJob(service, user, steps, JOB_NEW_COMPLETED_ENROLLMENTS, openLatestCompletedEnrollment);
    }


    public void triggerJobForUpdatedCompletedEnrollments(String service, String user, String insLookupTable,String enrLookupTable,
                                                         String evnLookupTable, Object mappingObj, List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String openLatestCompletedEnrollment,String startDate, String endDate)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        LinkedList<Step> steps = new LinkedList<>();
        steps.add(updatedEnrollmentWithEventsStep.get(insLookupTable,enrLookupTable, evnLookupTable, service, mappingObj, enrollmentsToIgnore,startDate, endDate));
        steps.add(updatedCompletedEnrollmentStep.get());
        triggerJob(service, user, steps, JOB_UPDATED_COMPLETED_ENROLLMENTS, openLatestCompletedEnrollment);
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
