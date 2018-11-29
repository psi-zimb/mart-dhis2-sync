package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DHIS_ACCEPTABLE_DATEFORMAT;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EnrollmentUtil.class, BatchUtil.class, EventUtil.class})
public class NewCompletedEnrollmentProcessorTest {
    private NewCompletedEnrollmentProcessor processor;

    private Date bahmniDate = new Date(Long.MIN_VALUE);
    private String eventDateCreated = "2018-10-25 13:46:23";
    private String enrollmentDateCreated = "2018-10-24 13:46:23";
    private String patientIdentifier = "NAH0001";
    private String orgUnit = "PSI-ZIMB-NAH";
    private String enrDate = "2018-10-24";
    private String eventDateValue = "2018-10-25";
    private String status = "COMPLETED";
    private String instanceId = "EmACSYDCxhu";
    private String program = "SxgCPPeiq3c";
    private String programStage = "m6Yfksc81Tg";
    private String orgUnitId = "ofaUrIe32";

    @Before
    public void setUp() throws Exception {
        processor = new NewCompletedEnrollmentProcessor();

        mockStatic(EnrollmentUtil.class);
        EnrollmentUtil.date = bahmniDate;

        mockStatic(EventUtil.class);
        EventUtil.date = bahmniDate;
        doNothing().when(EventUtil.class);
        EventUtil.addNewEventTracker(getTableRowObject());
        when(EventUtil.getElementsOfTypeDateTime()).thenReturn(Collections.singletonList("zJBTDNgXNu7"));

        mockStatic(BatchUtil.class);
        when(getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME))
                .thenReturn(eventDateValue);
        when(getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME)).thenReturn(enrDate);
        when(BatchUtil.hasValue(getMappingJsonObj().get("crptc"))).thenReturn(true);
        when(BatchUtil.hasValue(getMappingJsonObj().get("date_created_of_event"))).thenReturn(true);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForAPatientAndShouldUpdateEventUtilDateAndEnrUtilDateToBahmniDate() throws ParseException {
        Date enrollmentDate = getDate(enrollmentDateCreated);
        Date eventDate = getDate(eventDateCreated);
        when(getDateFromString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(eventDate);
        when(getDateFromString(enrollmentDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(enrollmentDate);

        processor.setMappingObj(getMappingJsonObj());

        ProcessedTableRow actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(enrollmentDate, EnrollmentUtil.date);
        assertEquals(eventDate, EventUtil.date);
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(getTableRowObject());
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForAPatientAndShouldNotUpdateEventUtilDateAndEnrUtilDateToBahmniDate() throws ParseException {
        Date existingEnrDate = getDate("2018-10-25 13:46:23");
        EnrollmentUtil.date = existingEnrDate;
        Date existingEventDate = getDate("2018-10-26 13:46:23");
        EventUtil.date = existingEventDate;
        Date enrollmentDate = getDate(enrollmentDateCreated);
        Date eventDate = getDate(eventDateCreated);
        when(getDateFromString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(eventDate);
        when(getDateFromString(enrollmentDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(enrollmentDate);

        processor.setMappingObj(getMappingJsonObj());

        ProcessedTableRow actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(existingEnrDate, EnrollmentUtil.date);
        assertEquals(existingEventDate, EventUtil.date);
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(getTableRowObject());
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForAPatientAndShouldUpdateOnlyEventUtilDateToBahmniDate() throws ParseException {
        Date existingEventDate = getDate("2018-10-26 13:46:23");
        EventUtil.date = existingEventDate;
        Date enrollmentDate = getDate(enrollmentDateCreated);
        Date eventDate = getDate(eventDateCreated);
        when(getDateFromString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(eventDate);
        when(getDateFromString(enrollmentDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(enrollmentDate);

        processor.setMappingObj(getMappingJsonObj());

        ProcessedTableRow actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(enrollmentDate, EnrollmentUtil.date);
        assertEquals(existingEventDate, EventUtil.date);
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(getTableRowObject());
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForAPatientAndShouldUpdateOnlyEnrollmentUtilDateToBahmniDate() throws ParseException {
        Date existingEnrDate = getDate("2018-10-25 13:46:23");
        EnrollmentUtil.date = existingEnrDate;
        Date enrollmentDate = getDate(enrollmentDateCreated);
        Date eventDate = getDate(eventDateCreated);
        when(getDateFromString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(eventDate);
        when(getDateFromString(enrollmentDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(enrollmentDate);

        processor.setMappingObj(getMappingJsonObj());

        ProcessedTableRow actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(existingEnrDate, EnrollmentUtil.date);
        assertEquals(eventDate, EventUtil.date);
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(getTableRowObject());
    }

    @Test
    public void shouldChangeTheFormatForDataValueIfTheDataTypeIsTIMESTAMP() throws ParseException {
        Date enrollmentDate = getDate(enrollmentDateCreated);
        Date eventDate = getDate(eventDateCreated);

        when(getDateFromString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(eventDate);
        when(getDateFromString(enrollmentDateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(enrollmentDate);
        when(getFormattedDateString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME, DHIS_ACCEPTABLE_DATEFORMAT))
                .thenReturn("2018-10-25T13:46:23");

        processor.setMappingObj(getMappingJsonObj());

        JsonObject tableRowObject = getTableRowObject();
        tableRowObject.addProperty("date_created_of_event", eventDateCreated);
        ProcessedTableRow actual = processor.process(tableRowObject);

        mockVerify();

        ProcessedTableRow expected = getExpected();
        expected.getPayLoad().getEvents().get(0).getDataValues().put("zJBTDNgXNu7", "2018-10-25T13:46:23");
        assertEquals(expected, actual);
        assertEquals(enrollmentDate, EnrollmentUtil.date);
        assertEquals(eventDate, EventUtil.date);
        verifyStatic(times(1));
        EventUtil.addNewEventTracker(tableRowObject);
    }

    private void mockVerify() {
        verifyStatic(times(1));
        getDateFromString(eventDateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(1));
        getDateFromString(enrollmentDateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(1));
        getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);
        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);
    }

    private Date getDate(String date) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATEFORMAT_WITH_24HR_TIME);
        return simpleDateFormat.parse(date);
    }

    private JsonObject getTableRowObject() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("incident_date", enrDate);
        tableRowObject.addProperty("enrollment_date_created", enrollmentDateCreated);
        tableRowObject.addProperty("program_unique_id", "1");
        tableRowObject.addProperty("\"Patient_Identifier\"", patientIdentifier);
        tableRowObject.addProperty("program", program);
        tableRowObject.addProperty("program_stage", programStage);
        tableRowObject.addProperty("\"OrgUnit\"", orgUnit);
        tableRowObject.addProperty("event_date", eventDateValue);
        tableRowObject.addProperty("status", status);
        tableRowObject.addProperty("event_unique_id", "1");
        tableRowObject.addProperty("enrollment_date", enrDate);
        tableRowObject.addProperty("date_created", eventDateCreated);
        tableRowObject.addProperty("crptc", "no");
        tableRowObject.addProperty("orgunit_id", orgUnitId);
        tableRowObject.addProperty("instance_id", instanceId);

        return tableRowObject;
    }

    private JsonObject getMappingJsonObj() {
        JsonObject mappingJsonObj = new JsonObject();
        mappingJsonObj.addProperty("crptc", "gXNu7zJBTDN");
        mappingJsonObj.addProperty("date_created_of_event", "zJBTDNgXNu7");

        return mappingJsonObj;
    }

    private ProcessedTableRow getExpected() {
        return new ProcessedTableRow(patientIdentifier, getEnrollmentPayLoad());
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad() {
        Map<String, String> dataValues = new HashMap<>();
        dataValues.put("gXNu7zJBTDN", "no");

        Event event = new Event(
                "",
                instanceId,
                "",
                program,
                programStage,
                orgUnitId,
                eventDateValue,
                status,
                dataValues
        );
        return new EnrollmentAPIPayLoad(
                "",
                instanceId,
                program,
                orgUnitId,
                enrDate,
                enrDate,
                "ACTIVE",
                "1",
                Collections.singletonList(event)
        );
    }
}
