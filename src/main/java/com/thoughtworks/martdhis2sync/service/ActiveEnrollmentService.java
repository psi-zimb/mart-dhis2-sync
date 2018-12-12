package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.step.ActiveEnrollmentStep;
import com.thoughtworks.martdhis2sync.step.NewActiveEnrollmentWithEventsStep;
import com.thoughtworks.martdhis2sync.step.UpdatedActiveEnrollmentWithEventsStep;
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
public class ActiveEnrollmentService {

    @Autowired
    private NewActiveEnrollmentWithEventsStep newEnrollmentWithEventsStep;

    @Autowired
    private UpdatedActiveEnrollmentWithEventsStep updatedEnrollmentWithEventsStep;

    @Autowired
    private ActiveEnrollmentStep activeEnrollmentStep;

    @Autowired
    private JobService jobService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "Active Enrollments: ";
    private static final String JOB_NEW_ACTIVE_ENROLLMENTS = "New Active Enrollments";
    private static final String JOB_UPDATED_ACTIVE_ENROLLMENTS = "Updated Active Enrollments";

    public void triggerJobForNewActiveEnrollments(String service, String user, String enrLookupTable,
                                                  String evnLookupTable, Object mappingObj)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        LinkedList<Step> steps = new LinkedList<>();
        steps.add(newEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, service, mappingObj));
        steps.add(activeEnrollmentStep.get());
        triggerJob(service, user, steps, JOB_NEW_ACTIVE_ENROLLMENTS);
    }

    public void triggerJobForUpdatedActiveEnrollments(String service, String user, String enrLookupTable,
                                                      String evnLookupTable, Object mappingObj, List<EnrollmentAPIPayLoad> enrollmentsToIgnore)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        LinkedList<Step> steps = new LinkedList<>();
        steps.add(updatedEnrollmentWithEventsStep.get(enrLookupTable, evnLookupTable, service, mappingObj, enrollmentsToIgnore));
        steps.add(activeEnrollmentStep.get());
        triggerJob(service, user, steps, JOB_UPDATED_ACTIVE_ENROLLMENTS);
    }

    private void triggerJob(String service, String user, LinkedList<Step> steps, String jobName)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException,
            JobInstanceAlreadyCompleteException, SyncFailedException {
        try {
            jobService.triggerJob(service, user, jobName, steps);
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }
}
