package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;

import java.sql.SQLException;
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
@PrepareForTest({BatchUtil.class})
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

    private EnrollmentAPIPayLoad payLoad1;

    private String instanceId1 = "instance1";
    private String enrDate = "2018-10-13";
    private String date = "2018-10-12 13:00:00";

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
        Map<String, String> dataValues2 = new HashMap<>();
        dataValues2.put("gXNu7zJBTDN", "yes");
        dataValues2.put("jkEjtKqlJtN", "event value2");
        Map<String, String> dataValues3 = new HashMap<>();
        dataValues3.put("gXNu7zJBTDN", "yes");
        dataValues3.put("jkEjtKqlJtN", "event value3");
        Event event1 = getEvents(instanceId1, date, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1");
        EnrollmentUtil.enrollmentsToSaveInTracker.add(payLoad1);
    }

    @After
    public void tearDown() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldUpdateTrackers() throws Exception {

        when(trackersHandler.insertInEnrollmentTracker("superman")).thenReturn(1);
        when(trackersHandler.insertInEventTracker("superman")).thenReturn(1);

        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(1)).insertInEnrollmentTracker("superman");
        verify(trackersHandler, times(1)).insertInEventTracker("superman");
    }

    @Test
    public void shouldLogMessageWhenFailedToInsertIntoTrackers() throws Exception {
        when(trackersHandler.insertInEnrollmentTracker("superman")).thenReturn(1);
        when(trackersHandler.insertInEventTracker("superman")).thenThrow(new SQLException("can't get database connection"));

        try {
            tasklet.execute(stepContribution, chunkContext);
        } catch (Exception e) {
            verify(logger, times(1)).error("NEW ACTIVE ENROLLMENT SYNC: Exception occurred " +
                    "while inserting Event UIDs:can't get database connection");
        }
    }

    @Test
    public void shouldNotCallSyncRepositoryIfTrackerIsEmpty() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        tasklet.execute(stepContribution, chunkContext);

        verify(trackersHandler, times(0)).insertInEnrollmentTracker("superman");
        verify(trackersHandler, times(0)).insertInEventTracker("superman");
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
