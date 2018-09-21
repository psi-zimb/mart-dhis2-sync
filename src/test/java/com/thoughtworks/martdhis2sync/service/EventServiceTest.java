package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.step.EventStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class EventServiceTest {
    @Mock
    private JobService jobService;

    @Mock
    private EventStep eventStep;

    private EventService eventService;

    private String jobName = "Sync Event";
    private String lookUpTable = "patient_event";
    private String user = "testUser";
    private String programName = "Event Service";
    private String mappingObj = "";


    @Before
    public void setUp() throws Exception {
        eventService = new EventService();
        setValuesForMemberFields(eventService, "eventStep", eventStep);
        setValuesForMemberFields(eventService, "jobService", jobService);
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {

        doNothing().when(jobService)
                .triggerJob(programName, user, lookUpTable, jobName, eventStep, mappingObj);

        eventService.triggerJob(programName, user, lookUpTable, mappingObj);

        verify(jobService, times(1))
                .triggerJob(programName, user, lookUpTable, jobName, eventStep, mappingObj);
    }

    @Test(expected = JobExecutionAlreadyRunningException.class)
    public void shouldThrowJobExecutionAlreadyRunningException() throws Exception {

        doThrow(JobExecutionAlreadyRunningException.class).when(jobService)
                .triggerJob(programName, user, lookUpTable, jobName, eventStep, mappingObj);

        eventService.triggerJob(programName, user, lookUpTable, mappingObj);
    }
}