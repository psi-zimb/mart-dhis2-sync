package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValueForStaticField;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DHIS_ACCEPTABLE_DATEFORMAT;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BatchUtil.class)
public class EventUtilTest {

    @Before
    public void setUp() throws Exception {
        mockStatic(BatchUtil.class);
        JsonObject tableRow = getTableRow("");
        when(BatchUtil.getUnquotedString(tableRow.get("instance_id").toString())).thenReturn("nLGUkAmW1YS");
        when(BatchUtil.getUnquotedString(tableRow.get("program").toString())).thenReturn("UoZQdIJuv1R");
        when(BatchUtil.getUnquotedString(tableRow.get("event_unique_id").toString())).thenReturn("m6Yfksc81Tg");
        when(BatchUtil.getUnquotedString(tableRow.get("program_stage").toString())).thenReturn("PiGF5LQjHrW");
    }

    @Test
    public void shouldAddTheGivenObjectToExistingEventTracker() throws NoSuchFieldException, IllegalAccessException {
        JsonObject tableRow = getTableRow("8hUkh8G");
        when(BatchUtil.hasValue(tableRow.get("event_id"))).thenReturn(true);
        when(BatchUtil.getUnquotedString(tableRow.get("event_id").toString())).thenReturn("8hUkh8G");
        setValueForStaticField(EventUtil.class, "existingEventTrackers", new ArrayList<>());

        assertEquals(0, EventUtil.getExistingEventTrackers().size());

        EventUtil.addExistingEventTracker(tableRow);

        assertEquals(1, EventUtil.getExistingEventTrackers().size());
    }

    @Test
    public void shouldAddTheGivenObjectToNewEventTracker() throws NoSuchFieldException, IllegalAccessException {
        JsonObject tableRow = getTableRow("");
        when(BatchUtil.hasValue(tableRow.get("event_id"))).thenReturn(false);
        setValueForStaticField(EventUtil.class, "newEventTrackers", new ArrayList<>());

        assertEquals(0, EventUtil.getNewEventTrackers().size());

        EventUtil.addNewEventTracker(tableRow);

        assertEquals(1, EventUtil.getNewEventTrackers().size());
    }

    @Test
    public void shouldReturnCollatedList() throws NoSuchFieldException, IllegalAccessException {
        EventTracker existingTracker = mock(EventTracker.class);
        EventTracker newTracker = mock(EventTracker.class);
        List<EventTracker> existingList = Collections.singletonList(existingTracker);
        List<EventTracker> newList = Collections.singletonList(newTracker);
        setValueForStaticField(EventUtil.class, "existingEventTrackers", existingList);
        setValueForStaticField(EventUtil.class, "newEventTrackers", newList);

        List<EventTracker> eventTrackers = EventUtil.getEventTrackers();

        List<EventTracker> expected = new ArrayList<>();
        expected.add(newTracker);
        expected.add(existingTracker);
        assertEquals(expected, eventTrackers);
    }

    @Test
    public void shouldClearTheLists() throws NoSuchFieldException, IllegalAccessException {
        EventTracker existingTracker = mock(EventTracker.class);
        EventTracker newTracker = mock(EventTracker.class);
        List<EventTracker> existingList = new ArrayList<>();
        existingList.add(existingTracker);
        List<EventTracker> newList = new ArrayList<>();
        newList.add(newTracker);
        setValueForStaticField(EventUtil.class, "existingEventTrackers", existingList);
        setValueForStaticField(EventUtil.class, "newEventTrackers", newList);

        EventUtil.resetEventTrackersList();

        assertEquals(0, EventUtil.getNewEventTrackers().size());
        assertEquals(0, EventUtil.getExistingEventTrackers().size());
    }

    @Test
    public void shouldReturnDataValue() throws ParseException {
        EventUtil.setElementsOfTypeDateTime(new ArrayList<>());
        JsonObject mappingJsonObj = new JsonObject();
        mappingJsonObj.addProperty("crptc", "gXNu7zJBTDN");

        String eventId = "qweURiW";
        JsonObject tableRow = getTableRow(eventId);
        tableRow.addProperty("crptc", "yes");
        tableRow.addProperty("self_testing", "true");

        when(BatchUtil.hasValue(mappingJsonObj.get("crptc"))).thenReturn(true);

        Map<String, String> dataValues = EventUtil.getDataValues(tableRow, mappingJsonObj);

        Assert.assertEquals("yes", dataValues.get("gXNu7zJBTDN"));

    }

    @Test
    public void shouldChangeTheFormatForDataValueIfTheDataTypeIsTIMESTAMP() throws ParseException {
        EventUtil.setElementsOfTypeDateTime(Collections.singletonList("gXNu7zJBTDN"));
        JsonObject mappingJsonObj = new JsonObject();
        mappingJsonObj.addProperty("crptc", "gXNu7zJBTDN");

        String eventId = "qweURiW";
        JsonObject tableRow = getTableRow(eventId);
        String date = "2019-10-12 10:22:22";
        String dhisTimeStamp = "2019-10-12T10:22:22";
        tableRow.addProperty("crptc", date);

        when(BatchUtil.hasValue(mappingJsonObj.get("crptc"))).thenReturn(true);
        when(BatchUtil.getFormattedDateString(date, DATEFORMAT_WITH_24HR_TIME, DHIS_ACCEPTABLE_DATEFORMAT))
                .thenReturn(dhisTimeStamp);

        Map<String, String> dataValues = EventUtil.getDataValues(tableRow, mappingJsonObj);

        Assert.assertEquals(dhisTimeStamp, dataValues.get("gXNu7zJBTDN"));

    }

    @Test
    public void shouldGetNewEventsFirstFollowedByUpdates() {
        Event event1 = new Event("", "instance1", "ertAdfd", "JKrtlAL", "olkjAlkfs",
                "lfdsHterljL","2019-10-12", "COMPLETED", "1", new HashMap<>()
        );
        Event event2 = new Event("eventId", "instance1", "ertAdfd", "JKrtlAL", "olkjAlkfs",
                "lfdsHterljL","2019-10-12", "COMPLETED", "1", new HashMap<>()
        );
        Event event3 = new Event("", "instance1", "ertAdfd", "JKrtlAL", "olkjAlkfs",
                "lfdsHterljL","2019-10-12", "COMPLETED", "1", new HashMap<>()
        );
        List<Event> givenList = new LinkedList<>();
        givenList.add(event1);
        givenList.add(event2);
        givenList.add(event3);

        List<Event> events = EventUtil.placeNewEventsFirst(givenList);
        Assert.assertEquals(event1, events.get(0));
        Assert.assertEquals(event3, events.get(1));
        Assert.assertEquals(event2, events.get(2));
    }

    private JsonObject getTableRow(String eventId) {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("event_id", eventId);
        tableRowObject.addProperty("instance_id", "nLGUkAmW1YS");
        tableRowObject.addProperty("program", "UoZQdIJuv1R");
        tableRowObject.addProperty("program_stage", "m6Yfksc81Tg");
        tableRowObject.addProperty("event_unique_id", "PiGF5LQjHrW");

        return tableRowObject;
    }
}
