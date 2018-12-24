package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class UpdatedActiveEnrollmentTaskletTest {
    @Mock
    private Logger logger;

    @Mock
    private StepContribution stepContribution;

    @Mock
    private ChunkContext chunkContext;

    @Mock
    private StepContext stepContext;

    @Mock
    private TrackersHandler trackersHandler;

    private UpdatedActiveEnrollmentTasklet tasklet;

    private EnrollmentAPIPayLoad payLoad;

    private EventTracker eventTracker;

    private String instanceId1 = "instance1";
    private String enrDate = "2018-10-13";
    private String date = "2018-10-12 13:00:00";
    private String logPrefix = "UPDATED ACTIVE ENROLLMENT TASKLET: ";

    @Before
    public void setUp() throws Exception {
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("user", "superman");

        tasklet = new UpdatedActiveEnrollmentTasklet();

        setValuesForMemberFields(tasklet, "logger", logger);
        setValuesForMemberFields(tasklet, "trackersHandler", trackersHandler);

        when(chunkContext.getStepContext()).thenReturn(stepContext);
        when(stepContext.getJobParameters()).thenReturn(jobParams);

        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");
        Event event1 = getEvents(instanceId1, date, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        payLoad = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1");
        EnrollmentUtil.enrollmentsToSaveInTracker.add(payLoad);

        eventTracker = new EventTracker("ieux8w6gn", instanceId1, "psuenc33", "11", "Uhyf56yg");
        EventUtil.eventsToSaveInTracker.add(eventTracker);
    }

    @After
    public void tearDown() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldUpdateTrackers() throws Exception {
        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(1)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallTrackerHandlerIfTrackerIsEmpty() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EventUtil.eventsToSaveInTracker.clear();

        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(0)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(0)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallOnlyUpdateEnrollmentTrackerIfEventTrackerIsEmpty() throws Exception {
        EventUtil.eventsToSaveInTracker.clear();

        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(0)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallOnlyUpdateEventTrackerIfEnrollmentTrackerIsEmpty() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();

        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(0)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(1)).insertInEventTracker("superman", logPrefix, logger);
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, List<Event> events, String programUniqueId) {
        return new EnrollmentAPIPayLoad(
                "",
                instanceId,
                "xhjKKwoq",
                "jSsoNjesL",
                enrDate,
                enrDate,
                "ACTIVE",
                programUniqueId,
                events
        );
    }

    private Event getEvents(String instanceId, String eventDate, Map<String, String> dataValues, String eventUniqueId) {
        return new Event(
                "",
                instanceId,
                "",
                "xhjKKwoq",
                "FJTkwmaP",
                "jSsoNjesL",
                eventDate,
                "COMPLETED",
                eventUniqueId,
                dataValues
        );
    }

}
