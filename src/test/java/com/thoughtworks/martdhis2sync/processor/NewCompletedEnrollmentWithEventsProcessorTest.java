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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EnrollmentUtil.class, BatchUtil.class, EventUtil.class})
public class NewCompletedEnrollmentWithEventsProcessorTest {
    private NewCompletedEnrollmentWithEventsProcessor processor;

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
    Map<String, String> dataValues = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        dataValues.put("gXNu7zJBTDN", "no");

        processor = new NewCompletedEnrollmentWithEventsProcessor();

        mockStatic(EnrollmentUtil.class);
        mockStatic(EventUtil.class);

        mockStatic(BatchUtil.class);
        when(getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME))
                .thenReturn(eventDateValue);
        when(getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME)).thenReturn(enrDate);
        when(hasValue(getMappingJsonObj().get("crptc"))).thenReturn(true);
        when(hasValue(getMappingJsonObj().get("date_created_of_event"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("date_created"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrollment_date_created"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("event_unique_id"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrolled_patient_identifier"))).thenReturn(true);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForAPatient() throws ParseException {
        JsonObject tableRowObject = getTableRowObjectWithEvent();
        JsonObject mappingJsonObj = getMappingJsonObj();

        doNothing().when(EventUtil.class);
        EventUtil.updateLatestEventDateCreated(eventDateCreated);
        doNothing().when(EnrollmentUtil.class);
        EnrollmentUtil.updateLatestEnrollmentDateCreated(enrollmentDateCreated);
        when(EventUtil.getDataValues(tableRowObject, mappingJsonObj)).thenReturn(dataValues);

        processor.setMappingObj(mappingJsonObj);

        ProcessedTableRow actual = processor.process(tableRowObject);

        verifyStatic(times(1));
        getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);
        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        assertEquals(getExpected(), actual);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyWithEmptyEventList() throws ParseException {
        JsonObject tableRowObject = getTableRowObjectWithoutEvent();
        JsonObject mappingJsonObj = getMappingJsonObj();

        doNothing().when(EventUtil.class);
        EventUtil.updateLatestEventDateCreated("");
        doNothing().when(EnrollmentUtil.class);
        EnrollmentUtil.updateLatestEnrollmentDateCreated(enrollmentDateCreated);
        when(EventUtil.getDataValues(tableRowObject, mappingJsonObj)).thenReturn(dataValues);

        processor.setMappingObj(mappingJsonObj);

        EnrollmentAPIPayLoad enrollmentAPIPayLoad = new EnrollmentAPIPayLoad(
                "",
                instanceId,
                program,
                orgUnitId,
                enrDate,
                enrDate,
                status,
                "1",
                Collections.emptyList()
        );
        ProcessedTableRow expected = new ProcessedTableRow(patientIdentifier, enrollmentAPIPayLoad);

        ProcessedTableRow actual = processor.process(tableRowObject);

        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        assertEquals(expected, actual);
    }

    private JsonObject getTableRowObjectWithEvent() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("incident_date", enrDate);
        tableRowObject.addProperty("enrollment_date_created", enrollmentDateCreated);
        tableRowObject.addProperty("program_unique_id", "1");
        tableRowObject.addProperty("enrollment_status", status);
        tableRowObject.addProperty("enr_date", enrDate);
        tableRowObject.addProperty("enrolled_program", program);
        tableRowObject.addProperty("enrolled_patient_identifier", patientIdentifier);
        tableRowObject.addProperty("Patient_Identifier", patientIdentifier);
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

    private JsonObject getTableRowObjectWithoutEvent() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("incident_date", enrDate);
        tableRowObject.addProperty("enrollment_date_created", enrollmentDateCreated);
        tableRowObject.addProperty("program_unique_id", "1");
        tableRowObject.addProperty("enrollment_status", status);
        tableRowObject.addProperty("enr_date", enrDate);
        tableRowObject.addProperty("enrolled_program", program);
        tableRowObject.addProperty("enrolled_patient_identifier", patientIdentifier);
        tableRowObject.addProperty("Patient_Identifier", (String) null);
        tableRowObject.addProperty("program", (String) null);
        tableRowObject.addProperty("program_stage", (String) null);
        tableRowObject.addProperty("\"OrgUnit\"", (String) null);
        tableRowObject.addProperty("event_date", (String) null);
        tableRowObject.addProperty("status", (String) null);
        tableRowObject.addProperty("event_unique_id", (String) null);
        tableRowObject.addProperty("enrollment_date", (String) null);
        tableRowObject.addProperty("date_created", (String) null);
        tableRowObject.addProperty("crptc", (String) null);
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

        Event event = new Event(
                "",
                instanceId,
                "",
                program,
                programStage,
                orgUnitId,
                eventDateValue,
                status,
                "1",
                dataValues
        );
        return new EnrollmentAPIPayLoad(
                "",
                instanceId,
                program,
                orgUnitId,
                enrDate,
                enrDate,
                status,
                "1",
                Collections.singletonList(event)
        );
    }
}
