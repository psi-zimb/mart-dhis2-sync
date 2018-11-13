package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.DataElementsUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EventUtil.class, BatchUtil.class, DataElementsUtil.class})
public class EventProcessorTest {
    @Mock
    private Date eventDate;

    private EventProcessor processor;
    private Date bahmniDate = new Date(Long.MIN_VALUE);
    private String dateCreated = "2018-02-02 13:46:23";
    private String dhisAcceptableFormat = "2018-02-02T13:46:23";
    private String dateWithoutTime = "2018-02-02";
    private String dataValues = "{\"dataElement\": \"gXNu7zJBTDN\", \"value\": \"no\"}, {\"dataElement\": \"zJBTDNgXNu7\", \"value\": \"" + dhisAcceptableFormat + "\"}";
    private List<String> dataElementIds = new LinkedList<>();

    @Before
    public void setUp() throws Exception {
        processor = new EventProcessor();
        dataElementIds.add("zJBTDNgXNu7");
        dataElementIds.add("XNu7TDNgzJB");

        mockStatic(EventUtil.class);
        doNothing().when(EventUtil.class);
        EventUtil.addExistingEventTracker(getTableRowObject());
        doNothing().when(EventUtil.class);
        EventUtil.addNewEventTracker(getTableRowObject());
        EventUtil.date = eventDate;

        mockStatic(BatchUtil.class);
        mockStatic(DataElementsUtil.class);

        when(getUnquotedString("\"" + dateCreated + "\"")).thenReturn(dateCreated);
        when(BatchUtil.getDateFromString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME)).thenReturn(bahmniDate);
        when(getFormattedDateString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME, BatchUtil.DATEFORMAT_WITHOUT_TIME)).thenReturn(dateWithoutTime);
        when(BatchUtil.hasValue(getMappingJsonObj().get("crptc"))).thenReturn(true);
        when(BatchUtil.removeLastChar(any())).thenReturn(dataValues);
        when(DataElementsUtil.getDateTimeElements()).thenReturn(dataElementIds);
        when(getUnquotedString("\"gXNu7zJBTDN\"")).thenReturn("gXNu7zJBTDN");
        when(getUnquotedString("\"zJBTDNgXNu7\"")).thenReturn("zJBTDNgXNu7");
        when(getUnquotedString("\"" + dateCreated + "\"")).thenReturn(dateCreated);
        when(getFormattedDateString(dateCreated, DATEFORMAT_WITH_24HR_TIME, DHIS_ACCEPTABLE_DATEFORMAT)).thenReturn(dhisAcceptableFormat);
        when(getQuotedString(dhisAcceptableFormat)).thenReturn("\"" + dhisAcceptableFormat + "\"");
    }

    @Test
    public void shouldReturnEventRequestBodyForAPatientAndShouldUpdateEventUtilDateToTheBahmniDateCreatedAndShouldCallAddExistingEventTrackerWhenEventIdExists() {
        when(eventDate.compareTo(bahmniDate)).thenReturn(0);
        when(BatchUtil.hasValue(getTableRowObject().get("event_id"))).thenReturn(true);

        processor.setMappingObj(getMappingJsonObj());
        Object actual = processor.process(getTableRowObject());

        mockVerify();
        verifyStatic(times(1));
        EventUtil.addExistingEventTracker(getTableRowObject());

        assertEquals(getExpected(), actual);
        assertEquals(bahmniDate, EventUtil.date);
    }

    @Test
    public void shouldReturnEventRequestBodyForAPatientAndShouldUpdateEventUtilDateToBahmniDateAndCallAddNewEventTrackerWhenEventIdDoesNotExists() {
        when(eventDate.compareTo(bahmniDate)).thenReturn(0);
        when(BatchUtil.hasValue(getTableRowObject().get("event_id"))).thenReturn(false);

        processor.setMappingObj(getMappingJsonObj());
        Object actual = processor.process(getTableRowObject());

        mockVerify();
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(getTableRowObject());
        assertEquals(getExpected(), actual);
        assertEquals(bahmniDate, EventUtil.date);
    }

    @Test
    public void shouldReturnEventRequestBodyForAPatientAndShouldNotUpdateEventUtilDateToTheBahmniDateCreatedWhenETDIsGreaterAndShouldCallAddExistingEventTrackerWhenEventIdExists() {
        when(eventDate.compareTo(bahmniDate)).thenReturn(1);
        when(BatchUtil.hasValue(getTableRowObject().get("event_id"))).thenReturn(true);

        processor.setMappingObj(getMappingJsonObj());
        Object actual = processor.process(getTableRowObject());

        mockVerify();
        verifyStatic(times(1));
        EventUtil.addExistingEventTracker(getTableRowObject());

        assertEquals(getExpected(), actual);
        assertEquals(eventDate, EventUtil.date);
    }

    @Test
    public void shouldReturnEventRequestBodyForAPatientAndShouldNotUpdateEventUtilDateToBahmniDateWhenETDIsGreaterAndCallAddNewEventTrackerWhenEventIdDoesNotExists() {
        when(eventDate.compareTo(bahmniDate)).thenReturn(1);
        when(BatchUtil.hasValue(getTableRowObject().get("event_id"))).thenReturn(false);

        processor.setMappingObj(getMappingJsonObj());
        Object actual = processor.process(getTableRowObject());

        mockVerify();
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(getTableRowObject());
        assertEquals(getExpected(), actual);
        assertEquals(eventDate, EventUtil.date);
    }

    private void mockVerify() {
        verifyStatic(times(1));
        getUnquotedString("\"" + dateCreated + "\"");
        verifyStatic();
        BatchUtil.getDateFromString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(1));
        BatchUtil.hasValue(getMappingJsonObj().get("crptc"));
        verifyStatic(times(2));
        BatchUtil.hasValue(getTableRowObject().get("event_id"));
        verifyStatic(times(1));
        BatchUtil.removeLastChar(any());
        verifyStatic(times(1));
        DataElementsUtil.getDateTimeElements();
        verifyStatic(times(1));
        getUnquotedString("\"gXNu7zJBTDN\"");
    }

    private JsonObject getMappingJsonObj() {
        JsonObject mappingJsonObj = new JsonObject();
        mappingJsonObj.addProperty("crptc", "gXNu7zJBTDN");

        return mappingJsonObj;
    }

    private JsonObject getTableRowObject() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("event_id", "");
        tableRowObject.addProperty("instance_id", "nLGUkAmW1YS");
        tableRowObject.addProperty("enrollment_id", "PiGF5LQjHrW");
        tableRowObject.addProperty("program", "UoZQdIJuv1R");
        tableRowObject.addProperty("program_stage", "m6Yfksc81Tg");
        tableRowObject.addProperty("orgunit_id", "SxgCPPeiq3c");
        tableRowObject.addProperty("event_date", "2018-02-02");
        tableRowObject.addProperty("status", "ACTIVE");
        tableRowObject.addProperty("date_created", dateCreated);
        tableRowObject.addProperty("crptc", "no");

        return tableRowObject;
    }

    private String getExpected() {
        return "{" +
                    "\"event\": \"\", " +
                    "\"trackedEntityInstance\": \"nLGUkAmW1YS\", " +
                    "\"enrollment\": \"PiGF5LQjHrW\", " +
                    "\"program\": \"UoZQdIJuv1R\", " +
                    "\"programStage\": \"m6Yfksc81Tg\", " +
                    "\"orgUnit\": \"SxgCPPeiq3c\", " +
                    "\"eventDate\": \"null\", " +
                    "\"status\": \"ACTIVE\", " +
                    "\"dataValues\":" +
                        "["+
                            dataValues +
                        "]" +
                "}";
    }
}
