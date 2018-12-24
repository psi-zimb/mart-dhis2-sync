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
public class NewActiveEnrollmentTaskletTest {
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

    private NewActiveEnrollmentTasklet tasklet;

    private String logPrefix = "NEW ACTIVE ENROLLMENT TASKLET: ";

    @Before
    public void setUp() throws Exception {
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("user", "superman");

        tasklet = new NewActiveEnrollmentTasklet();

        setValuesForMemberFields(tasklet, "logger", logger);
        setValuesForMemberFields(tasklet, "trackersHandler", trackersHandler);

        when(chunkContext.getStepContext()).thenReturn(stepContext);
        when(stepContext.getJobParameters()).thenReturn(jobParams);

        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(dataValues1);
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);

        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(events1);

        EnrollmentUtil.enrollmentsToSaveInTracker.add(payLoad1);

        EventTracker eventTracker = new EventTracker("eventId", "instance1", "xhjKKwoq", "1", "FJTkwmaP");
        EventUtil.eventsToSaveInTracker.add(eventTracker);
    }

    @Test
    public void shouldUpdateTrackers() {
        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(1)).insertInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(1)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallTrackerIfEnrollmentTrackerAndEventTrackersAreEmpty() {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EventUtil.eventsToSaveInTracker.clear();
        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(0)).insertInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(0)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallInsertIntoEventTrackerIfEventTrackerIsEmpty() throws Exception {
        EventUtil.eventsToSaveInTracker.clear();
        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(1)).insertInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(0)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallInsertIntoEnrollmentTrackerIfEnrollmentTrackerIsEmpty() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(0)).insertInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(1)).insertInEventTracker("superman", logPrefix, logger);
    }

    @After
    public void tearDown() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EventUtil.eventsToSaveInTracker.clear();
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(List<Event> events) {
        return new EnrollmentAPIPayLoad(
                "enrollmentId1",
                "instance1",
                "xhjKKwoq",
                "jSsoNjesL",
                "2018-10-13",
                "2018-10-13",
                "ACTIVE",
                "1",
                events
        );
    }

    private Event getEvents(Map<String, String> dataValues) {
        return new Event(
                "eventId1",
                "instance1",
                "",
                "xhjKKwoq",
                "FJTkwmaP",
                "jSsoNjesL",
                "2018-10-12 13:00:00",
                "COMPLETED",
                "1",
                dataValues
        );
    }
}
