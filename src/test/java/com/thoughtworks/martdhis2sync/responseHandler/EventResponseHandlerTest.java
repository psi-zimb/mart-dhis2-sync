package com.thoughtworks.martdhis2sync.responseHandler;

import com.thoughtworks.martdhis2sync.model.Conflict;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.model.Response;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_WARNING;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
public class EventResponseHandlerTest {

    @Mock
    private LoggerService loggerService;

    @Mock
    private Logger logger;

    private String prefix = "New Completed Evens:";

    private EventResponseHandler responseHandler;

    private EnrollmentAPIPayLoad payLoad1;
    private EnrollmentAPIPayLoad payLoad2;
    private EnrollmentAPIPayLoad payLoad3;
    private Event event1;
    private Event event2;
    private Event event3;
    private String enrReference1 = "enrReference1";
    private String enrReference2 = "enrReference2";
    private String enrReference3 = "enrReference3";
    private String envReference1 = "envReference1";
    private String envReference2 = "envReference2";
    private String envReference3 = "envReference3";

    private String instanceId1 = "instance1";
    private String instanceId2 = "instance2";
    private String instanceId3 = "instance3";
    private String enrDate = "2018-10-13";
    private String eventDate = "2018-10-14";
    private Map<String, String> dataValues1 = new HashMap<>();
    private Map<String, String> dataValues2 = new HashMap<>();
    private Map<String, String> dataValues3 = new HashMap<>();


    @Before
    public void setUp() throws Exception {
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");
        dataValues2.put("gXNu7zJBTDN", "yes");
        dataValues2.put("jkEjtKqlJtN", "event value2");
        dataValues3.put("gXNu7zJBTDN", "yes");
        dataValues3.put("jkEjtKqlJtN", "event value3");

        responseHandler = new EventResponseHandler();

        setValuesForMemberFields(responseHandler, "loggerService", loggerService);
    }

    @Test
    public void shouldAddAllEventTrackerToTheEventsToSaveInTrackerWhenAllAreNewEvents() {
        EventUtil.eventsToSaveInTracker.clear();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        event2 = getEvents(instanceId2, eventDate, dataValues2, "2", "");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        payLoad2 = getEnrollmentPayLoad(instanceId2, enrDate, events2, "2", enrReference2);

        event3 = getEvents(instanceId3, eventDate, dataValues3, "3", "");
        List<Event> events3 = new LinkedList<>();
        events3.add(event3);
        payLoad3 = getEnrollmentPayLoad(instanceId3, enrDate, events3, "3", enrReference3);
        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1, payLoad2, payLoad3);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker("", instanceId2, "2");
        EventTracker eventTracker3 = getEventTracker("", instanceId3, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2, eventTracker3);

        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference1)),
                        1
                    )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference2, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference2)),
                        1
                    )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference3)),
                        1
                    )
                )
        );

        EventTracker expectedET1 = getEventTracker(envReference1, instanceId1, "1");
        EventTracker expectedET2 = getEventTracker(envReference2, instanceId2, "2");
        EventTracker expectedET3 = getEventTracker(envReference3, instanceId3, "3");

        List<EventTracker> expectedEventTracker = Arrays.asList(expectedET1, expectedET2, expectedET3);

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(expectedEventTracker, EventUtil.eventsToSaveInTracker);

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldNotAddEventTrackerToTheEventsToSaveInTrackerWhenAllAreUpdatedEvents() {
        EventUtil.eventsToSaveInTracker.clear();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        event2 = getEvents(instanceId2, eventDate, dataValues2, "2", "");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        payLoad2 = getEnrollmentPayLoad(instanceId2, enrDate, events2, "2", enrReference2);

        event3 = getEvents(instanceId3, eventDate, dataValues3, "3", "");
        List<Event> events3 = new LinkedList<>();
        events3.add(event3);
        payLoad3 = getEnrollmentPayLoad(instanceId3, enrDate, events3, "3", enrReference3);
        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1, payLoad2, payLoad3);

        EventTracker eventTracker1 = getEventTracker(envReference1, instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker(envReference2, instanceId2, "2");
        EventTracker eventTracker3 = getEventTracker(envReference3, instanceId3, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2, eventTracker3);

        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 0, 2, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(0, 2, 0, 0),
                                        null, new ArrayList<>(), envReference1)),
                        1
                    )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference2, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 0, 2, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(0, 2, 0, 0),
                                        null, new ArrayList<>(), envReference2)),
                        1
                    )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 0, 2, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(0, 2, 0, 0),
                                        null, new ArrayList<>(), envReference3)),
                        1
                    )
                )
        );

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(0, EventUtil.eventsToSaveInTracker.size());

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldLogDescriptionAndThrowExceptionWhenRequestBodyHasIncorrectProgramForNewEvents() {
        EventUtil.eventsToSaveInTracker.clear();
        List<Event> events1 = new LinkedList<>();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        event2 = getEvents(instanceId1, eventDate, dataValues2, "2", "");
        event3 = getEvents(instanceId1, eventDate, dataValues3, "3", "");
        events1.add(event1);
        events1.add(event2);
        events1.add(event3);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker("", instanceId1, "2");
        EventTracker eventTracker3 = getEventTracker("", instanceId1, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2, eventTracker3);

        String description = "Event.program does not point to a valid program: rleFtLk_1";
        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_ERROR, 0, 0, 0, 6,
                        Arrays.asList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        description, new ArrayList<>(), null),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        description, new ArrayList<>(), null),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        description, new ArrayList<>(), null)
                        ),
                        1
                    )
                )
        );

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(0, EventUtil.eventsToSaveInTracker.size());
        verify(logger, times(3)).error(prefix + description);
        verify(loggerService, times(3)).collateLogMessage(description);

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldUpdateReferencesForFirstAndThirdEventAndLogDescriptionForSecondEventWhenFirstEventHasCorrectProgramAndSecondHasIncorrectProgram() {
        EventUtil.eventsToSaveInTracker.clear();
        List<Event> events1 = new LinkedList<>();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        event2 = getEvents(instanceId1, eventDate, dataValues2, "2", "");
        event3 = getEvents(instanceId1, eventDate, dataValues3, "3", "");
        events1.add(event1);
        events1.add(event2);
        events1.add(event3);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker("", instanceId1, "2");
        EventTracker eventTracker3 = getEventTracker("", instanceId1, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2, eventTracker3);

        String description = "Event.program does not point to a valid program: incorrectProgram";
        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_ERROR, 0, 0, 0, 6,
                        Arrays.asList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference1),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        description, new ArrayList<>(), null),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference3)
                        ),
                        1
                )
                )
        );

        EventTracker expectedET1 = getEventTracker(envReference1, instanceId1, "1");
        EventTracker expectedET3 = getEventTracker(envReference3, instanceId1, "3");

        List<EventTracker> expectedEventTracker = Arrays.asList(expectedET1, expectedET3);

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(expectedEventTracker, EventUtil.eventsToSaveInTracker);
        verify(logger, times(1)).error(prefix + description);
        verify(loggerService, times(1)).collateLogMessage(description);

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldUpdateReferencesForSecondAndThirdEventAndLogDescriptionForFirstEventWhenFirstEventHasIncorrectProgramAndSecondHasCorrectProgram() {
        EventUtil.eventsToSaveInTracker.clear();
        List<Event> events1 = new LinkedList<>();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        event2 = getEvents(instanceId1, eventDate, dataValues2, "2", "");
        event3 = getEvents(instanceId1, eventDate, dataValues3, "3", "");
        events1.add(event1);
        events1.add(event2);
        events1.add(event3);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker("", instanceId1, "2");
        EventTracker eventTracker3 = getEventTracker("", instanceId1, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2, eventTracker3);

        String description = "Event.program does not point to a valid program: incorrectProgram";
        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_ERROR, 0, 0, 0, 6,
                        Arrays.asList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        description, new ArrayList<>(), null),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference2),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference3)
                        ),
                        1
                )
                )
        );

        EventTracker expectedET2 = getEventTracker(envReference2, instanceId1, "2");
        EventTracker expectedET3 = getEventTracker(envReference3, instanceId1, "3");

        List<EventTracker> expectedEventTracker = Arrays.asList(expectedET2, expectedET3);

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(expectedEventTracker, EventUtil.eventsToSaveInTracker);
        verify(logger, times(1)).error(prefix + description);
        verify(loggerService, times(1)).collateLogMessage(description);

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldLogConflictMessageAndDoNotUpdateTrackerWhenTheResponseHasErrorWithConflict() {
        EventUtil.eventsToSaveInTracker.clear();
        List<Event> events1 = new LinkedList<>();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        event2 = getEvents(instanceId1, eventDate, dataValues2, "2", "");
        event3 = getEvents(instanceId1, eventDate, dataValues3, "3", "");
        events1.add(event1);
        events1.add(event2);
        events1.add(event3);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker("", instanceId1, "2");
        EventTracker eventTracker3 = getEventTracker("", instanceId1, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2, eventTracker3);

        List<Conflict> conflicts = Collections.singletonList(new Conflict("jfDdErl", "value_not_true_only"));
        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_ERROR, 0, 0, 0, 6,
                        Arrays.asList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        null, conflicts, null),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        null, conflicts, null),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                                        new ImportCount(0, 0, 2, 0),
                                        null, conflicts, null)
                        ),
                        1
                )
                )
        );

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(0, EventUtil.eventsToSaveInTracker.size());
        verify(logger, times(3)).error(prefix + "jfDdErl: value_not_true_only");
        verify(loggerService, times(3)).collateLogMessage("jfDdErl: value_not_true_only");

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldLogConflictMessageAndUpdateTrackerWhenResponseHasWarning() {
        EventUtil.eventsToSaveInTracker.clear();
        List<Event> events1 = new LinkedList<>();
        event1 = getEvents("", eventDate, dataValues1, "1", "");
        events1.add(event1);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Collections.singletonList(payLoad1);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");

        List<EventTracker> eventTrackers = Collections.singletonList(eventTracker1);

        List<Conflict> conflicts = Collections.singletonList(new Conflict("jfDdErl", "value_not_true_only"));
        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_ERROR, 0, 0, 0, 2,
                        Arrays.asList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_WARNING,
                                        new ImportCount(0, 0, 2, 0),
                                        null, conflicts, null)
                        ),
                        1
                )
                )
        );

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(0, EventUtil.eventsToSaveInTracker.size());
        verify(logger, times(1)).error(prefix + "jfDdErl: value_not_true_only");
        verify(loggerService, times(1)).collateLogMessage("jfDdErl: value_not_true_only");

        EventUtil.eventsToSaveInTracker.clear();
    }

    @Test
    public void shouldAddOnlyNewToTheTrackerWhenComboEventsAreSynced() {
        EventUtil.eventsToSaveInTracker.clear();
        List<Event> events1 = new LinkedList<>();
        event1 = getEvents(instanceId1, eventDate, dataValues1, "1", "");
        event2 = getEvents(instanceId1, eventDate, dataValues2, "2", envReference2);
        event3 = getEvents(instanceId1, eventDate, dataValues3, "3", "");
        events1.add(event1);
        events1.add(event2);
        events1.add(event3);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrReference1);

        List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = Arrays.asList(payLoad1);

        EventTracker eventTracker1 = getEventTracker("", instanceId1, "1");
        EventTracker eventTracker2 = getEventTracker(envReference2, instanceId1, "2");
        EventTracker eventTracker3 = getEventTracker("", instanceId1, "3");

        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker3, eventTracker2);

        List<Conflict> conflicts = Collections.singletonList(new Conflict("jfDdErl", "value_not_true_only"));
        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 0, 2, 0, 4,
                        Arrays.asList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(2, 0, 0, 0),
                                        null, conflicts, envReference1),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(2, 0, 0, 0),
                                        null, conflicts, envReference3),
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                                        new ImportCount(0, 2, 0, 0),
                                        null, conflicts, envReference2)
                        ),
                        1
                )
                )
        );

        EventTracker expectedET1 = getEventTracker(envReference1, instanceId1, "1");
        EventTracker expectedET3 = getEventTracker(envReference3, instanceId1, "3");

        List<EventTracker> expectedEventTracker = Arrays.asList(expectedET1, expectedET3);

        responseHandler.process(enrollmentAPIPayLoads, importSummaries, eventTrackers, logger, prefix);

        assertEquals(expectedEventTracker, EventUtil.eventsToSaveInTracker);

        EventUtil.eventsToSaveInTracker.clear();
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, List<Event> events, String programUniqueId, String  enrollmentId) {
        return new EnrollmentAPIPayLoad(
                enrollmentId,
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

    private Event getEvents(String instanceId, String eventDate, Map<String, String> dataValues, String eventUniqueId, String eventId) {
        return new Event(
                eventId,
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

    private EventTracker getEventTracker(String eventId, String instanceId, String eventUniqueId) {
        return new EventTracker(
                eventId,
                instanceId,
                "xhjKKwoq",
                eventUniqueId,
                "FJTkwmaP"
        );
    }
}
