package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;

import java.io.SyncFailedException;
import java.util.Arrays;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class TEIServiceTest {

    @Mock
    private TrackedEntityInstanceStep instanceStep;

    @Mock
    private JobService jobService;

    private TEIService teiService;
    private List<String> searchableAttributes = Arrays.asList("UIC", "date_created");

    @Before
    public void setUp() throws Exception {
        teiService = new TEIService();
        setValuesForMemberFields(teiService, "trackedEntityInstanceStep", instanceStep);
        setValuesForMemberFields(teiService, "jobService", jobService);

        teiService.setSearchableAttributes(searchableAttributes);
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";
        String jobName = "Sync Tracked Entity Instance";

        doNothing().when(jobService).triggerJob(service, user, lookUpTable, jobName, instanceStep, mappingObj);

        teiService.triggerJob(service, user, lookUpTable, mappingObj);

        verify(jobService, times(1)).triggerJob(service, user, lookUpTable, jobName, instanceStep, mappingObj);
        verify(instanceStep, times(1)).setSearchableAttributes(searchableAttributes);
    }

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void shouldThrowJobExecutionAlreadyRunningException() throws Exception {
        String lookUpTable = "patient_identifier";
        Object mappingObj = "";
        String service = "serviceName";
        String user = "Admin";
        String jobName = "Sync Tracked Entity Instance";

        doThrow(JobExecutionAlreadyRunningException.class).when(jobService)
                .triggerJob(service, user, lookUpTable, jobName, instanceStep, mappingObj);

        try {
            teiService.triggerJob(service, user, lookUpTable, mappingObj);
        }catch (Exception e){
            verify(instanceStep, times(1)).setSearchableAttributes(searchableAttributes);
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

        doThrow(SyncFailedException.class).when(jobService)
                .triggerJob(service, user, lookUpTable, jobName, instanceStep, mappingObj);

        try{
            teiService.triggerJob(service, user, lookUpTable, mappingObj);
        }catch (Exception e){
            verify(instanceStep, times(1)).setSearchableAttributes(searchableAttributes);
            throw e;
        }
    }
}