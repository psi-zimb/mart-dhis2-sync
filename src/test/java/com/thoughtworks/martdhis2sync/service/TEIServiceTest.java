package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.PatientDAO;
import com.thoughtworks.martdhis2sync.model.EnrollmentDetails;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstance;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.http.ResponseEntity;

import java.io.SyncFailedException;
import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class TEIServiceTest {

    @Mock
    private TrackedEntityInstanceStep instanceStep;

    @Mock
    private JobService jobService;

    @Mock
    private Step step;

    @Mock
    private PatientDAO patientDAO;

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity<TrackedEntityInstanceResponse> responseEntity;

    @Mock
    private TrackedEntityInstanceResponse response;

    private TEIService teiService;
    private LinkedList<Step> steps = new LinkedList<>();

    @Before
    public void setUp() throws Exception {
        teiService = new TEIService();
        setValuesForMemberFields(teiService, "trackedEntityInstanceStep", instanceStep);
        setValuesForMemberFields(teiService, "jobService", jobService);
        setValuesForMemberFields(teiService, "patientDAO", patientDAO);
        setValuesForMemberFields(teiService, "syncRepository", syncRepository);

        steps.add(step);

        TEIUtil.setInstancesWithEnrollments(new HashMap<>());
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";
        String jobName = "Sync Tracked Entity Instance";
        List<String> searchableAttributes = Arrays.asList("UIC", "date_created");
        List<String> comparableAttributes = Arrays.asList("patient_id", "prepID");

        doNothing().when(jobService).triggerJob(service, user, jobName, steps, "");
        when(instanceStep.get(lookUpTable, service, mappingObj, searchableAttributes, comparableAttributes)).thenReturn(step);

        teiService.triggerJob(service, user, lookUpTable, mappingObj, searchableAttributes, comparableAttributes);

        verify(jobService, times(1)).triggerJob(service, user, jobName, steps, "");
    }

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void shouldThrowJobExecutionAlreadyRunningException() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";
        String jobName = "Sync Tracked Entity Instance";
        List<String> searchableAttributes = Arrays.asList("UIC", "date_created");
        List<String> comparableAttributes = Arrays.asList("patient_id", "prepID");

        when(instanceStep.get(lookUpTable, service, mappingObj, searchableAttributes, comparableAttributes)).thenReturn(step);
        doThrow(JobExecutionAlreadyRunningException.class).when(jobService)
                .triggerJob(service, user, jobName, steps, "");

        try {
            teiService.triggerJob(service, user, lookUpTable, mappingObj, searchableAttributes, comparableAttributes);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test(expected = SyncFailedException.class)
    public void shouldThrowSyncFailedException() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";
        String jobName = "Sync Tracked Entity Instance";
        List<String> searchableAttributes = Arrays.asList("UIC", "date_created");
        List<String> comparableAttributes = Arrays.asList("patient_id", "prepID");

        when(instanceStep.get(lookUpTable, service, mappingObj, searchableAttributes, comparableAttributes)).thenReturn(step);
        doThrow(SyncFailedException.class).when(jobService)
                .triggerJob(service, user, jobName, steps, "");

        try {
            teiService.triggerJob(service, user, lookUpTable, mappingObj, searchableAttributes, comparableAttributes);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void shouldHaveEmptyListForInstanceWithEnrollments() throws Exception {
        String enrollment = "enrollmentTable";
        String programName = "HTS";
        String eventTable = "eventTable";
        when(patientDAO.getDeltaEnrollmentInstanceIds(enrollment, eventTable, programName)).thenReturn(new ArrayList<>());

        teiService.getEnrollmentsForInstances(enrollment, eventTable, programName);

        assertEquals(0, TEIUtil.getInstancesWithEnrollments().size());
    }

    @Test
    public void shouldReturnEnrollmentsForTheGivenProgramAndGivenInstances() throws Exception {
        String enrollment = "enrollmentTable";
        String programName = "HTS";
        String eventTable = "eventTable";
        Map<String, Object> map1 = new HashMap<>();
        map1.put("instance_id", "instance1");
        map1.put("program", "program");

        Map<String, Object> map2 = new HashMap<>();
        map2.put("instance_id", "instance2");
        map2.put("program", "program");

        String url = "/api/trackedEntityInstances?" +
                "fields=trackedEntityInstance,enrollments[program,enrollment,enrollmentDate,completedDate,status]&" +
                "program=program&trackedEntityInstance=instance1;instance2";

        EnrollmentDetails enrollment1 = new EnrollmentDetails("program", "enrollment1", "2018-10-22", "2018-12-10", "COMPLETED");
        EnrollmentDetails enrollment2 = new EnrollmentDetails("program", "enrollment2", "2018-10-22", null, "ACTIVE");

        TrackedEntityInstance trackedEntityInstance1 = new TrackedEntityInstance();
        trackedEntityInstance1.setEnrollments(Arrays.asList(enrollment1, enrollment2));
        trackedEntityInstance1.setTrackedEntityInstance("instance1");
        TrackedEntityInstance trackedEntityInstance2 = new TrackedEntityInstance();
        trackedEntityInstance2.setTrackedEntityInstance("instance2");
        trackedEntityInstance2.setEnrollments(Collections.emptyList());

        when(patientDAO.getDeltaEnrollmentInstanceIds(enrollment, eventTable, programName)).thenReturn(Arrays.asList(map1, map2));
        when(syncRepository.getTrackedEntityInstances(url)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(response);
        when(response.getTrackedEntityInstances()).thenReturn(Arrays.asList(trackedEntityInstance1, trackedEntityInstance2));

        teiService.getEnrollmentsForInstances(enrollment, eventTable, programName);

        Map<String, List<EnrollmentDetails>> expected = new HashMap<>();
        expected.put("instance1", Arrays.asList(enrollment1, enrollment2));

        assertEquals(expected, TEIUtil.getInstancesWithEnrollments());
    }
}
