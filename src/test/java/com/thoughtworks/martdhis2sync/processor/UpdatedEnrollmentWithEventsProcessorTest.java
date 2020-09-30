package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.EnrollmentService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EnrollmentUtil.class, BatchUtil.class, EventUtil.class})
public class UpdatedEnrollmentWithEventsProcessorTest {

    @Mock
    private TEIService teiService;

    @Mock
    private EnrollmentService enrollmentService;

    private UpdatedEnrollmentWithEventsProcessor processor;

    private String eventDateCreated = "2018-10-25 13:46:23";
    private String enrollmentDateCreated = "2018-10-24 13:46:23";
    private String patientIdentifier = "NAH0001";
    private String orgUnit = "PSI-ZIMB-NAH";
    private String enrDate = "2018-10-24";
    private String eventDateValue = "2018-10-25";
    private String status = "COMPLETED";
    private String enrollmentId = "jlkEjrItnl";
    private String eventId = "jltHwrKtoanD";
    private String instanceId = "EmACSYDCxhu";
    private String program = "SxgCPPeiq3c";
    private String programStage = "m6Yfksc81Tg";
    private String orgUnitId = "ofaUrIe32";
    private String gender = "M";
    private String lastName = "M";

    private String mothersFirstName = "MomFirstName";
    private String dateOfBirth = "09-09-2020";
    private String districtOfBorth = "BULAWAYO";
    private String uic = "EEYT67RC";
    private String areYouTwin="True";
    Map<String, String> dataValues = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        dataValues.put("gXNu7zJBTDN", "no");

        processor = new UpdatedEnrollmentWithEventsProcessor();
        setValuesForMemberFields(processor, "teiService", teiService);
        mockStatic(EnrollmentUtil.class);
        mockStatic(EventUtil.class);

        mockStatic(BatchUtil.class);
        when(getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME))
                .thenReturn(eventDateValue);
        when(getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME)).thenReturn(enrDate);
        when(BatchUtil.hasValue(getMappingJsonObj().get("crptc"))).thenReturn(true);
        when(BatchUtil.hasValue(getMappingJsonObj().get("date_created_of_event"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrolled_patient_identifier"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("event_unique_id"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("date_created"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrollment_date_created"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("incident_date"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrollment_status"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("program_unique_id"))).thenReturn(true);

    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForATEIWhenEventIdPresent() throws Exception {
        JsonObject tableRowObject = getTableRowObjectWithEvent();
        JsonObject mappingJsonObj = getMappingJsonObj();

        tableRowObject.addProperty("event_id", eventId);

        doNothing().when(EventUtil.class);
        EventUtil.updateLatestEventDateCreated(eventDateCreated);
        doNothing().when(EnrollmentUtil.class);
        EnrollmentUtil.updateLatestEnrollmentDateCreated(enrollmentDateCreated, MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT);
        when(EventUtil.getDataValues(tableRowObject, mappingJsonObj)).thenReturn(dataValues);
        when(teiService.getTrackedEntityInstancesForUIC(uic)).thenReturn(getTrackedEntityInstance());
        when(teiService.instanceExistsInDHIS(any(),any())).thenReturn(true);
        when(enrollmentService.isTrackedEntityInstanceEnrolledToPreferredProgrsamForInstanceId(anyString())).thenReturn(true);

        processor.setMappingObj(mappingJsonObj);

        ProcessedTableRow actual = processor.process(tableRowObject);

        verifyStatic(times(1));
        getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);
        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        assertEquals(getExpected(getEvent(eventId)), actual);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyWhenEventIdAbsent() throws Exception {
        JsonObject tableRowObject = getTableRowObjectWithEvent();
        JsonObject mappingJsonObj = getMappingJsonObj();

        tableRowObject.addProperty("event_id", "");

        doNothing().when(EventUtil.class);
        EventUtil.updateLatestEventDateCreated(eventDateCreated);
        doNothing().when(EnrollmentUtil.class);
        EnrollmentUtil.updateLatestEnrollmentDateCreated(enrollmentDateCreated, MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT);
        when(EventUtil.getDataValues(tableRowObject, mappingJsonObj)).thenReturn(dataValues);
        when(teiService.getTrackedEntityInstancesForUIC(uic)).thenReturn(getTrackedEntityInstance());
        when(teiService.instanceExistsInDHIS(any(),any())).thenReturn(true);
        when(enrollmentService.isTrackedEntityInstanceEnrolledToPreferredProgrsamForInstanceId(anyString())).thenReturn(true);

        processor.setMappingObj(mappingJsonObj);

        ProcessedTableRow actual = processor.process(tableRowObject);

        verifyStatic(times(1));
        getFormattedDateString(eventDateValue, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);
        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        assertEquals(getExpected(getEvent("")), actual);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyWithEmptyEventsList() throws Exception {
        JsonObject tableRowObject = getTableRowObjectWithoutEvent();
        JsonObject mappingJsonObj = getMappingJsonObj();

        doNothing().when(EventUtil.class);
        EventUtil.updateLatestEventDateCreated(eventDateCreated);
        doNothing().when(EnrollmentUtil.class);
        EnrollmentUtil.updateLatestEnrollmentDateCreated(enrollmentDateCreated, MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT);
        when(EventUtil.getDataValues(tableRowObject, mappingJsonObj)).thenReturn(dataValues);
        when(teiService.getTrackedEntityInstancesForUIC(uic)).thenReturn(getTrackedEntityInstance());
        when(teiService.instanceExistsInDHIS(any(),any())).thenReturn(true);
        when(enrollmentService.isTrackedEntityInstanceEnrolledToPreferredProgrsamForInstanceId(anyString())).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrolled_program"))).thenReturn(true);

        processor.setMappingObj(mappingJsonObj);

        ProcessedTableRow actual = processor.process(tableRowObject);

        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        ProcessedTableRow expected = getExpected(getEvent(""));
        expected.getPayLoad().setEvents(new LinkedList<>());
        assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyWhenOnlyEventAreThere() throws Exception {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("event_program_incident_date", enrDate);
        tableRowObject.addProperty("event_program_unique_id", "1");
        tableRowObject.addProperty("event_program_status", status);
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
        tableRowObject.addProperty("enrollment_id", enrollmentId);
        tableRowObject.addProperty("uic", uic);
        tableRowObject.addProperty("Gender", gender);
        tableRowObject.addProperty("Mothers_First_Name", mothersFirstName);
        tableRowObject.addProperty("Date_of_Birth", dateOfBirth);
        tableRowObject.addProperty("Last_Name", lastName);
        tableRowObject.addProperty("District_of_Birth", districtOfBorth);

        JsonObject mappingJsonObj = getMappingJsonObj();

        doNothing().when(EventUtil.class);
        EventUtil.updateLatestEventDateCreated(eventDateCreated);
        doNothing().when(EnrollmentUtil.class);
        EnrollmentUtil.updateLatestEnrollmentDateCreated(enrollmentDateCreated, MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT);
        when(EventUtil.getDataValues(tableRowObject, mappingJsonObj)).thenReturn(dataValues);
        when(hasValue(getTableRowObjectWithEvent().get("enrolled_program"))).thenReturn(true);
        when(hasValue(getTableRowObjectWithEvent().get("enrollment_status"))).thenReturn(false);
        when(teiService.getTrackedEntityInstancesForUIC(uic)).thenReturn(getTrackedEntityInstance());
        when(teiService.instanceExistsInDHIS(any(),any())).thenReturn(true);
        when(enrollmentService.isTrackedEntityInstanceEnrolledToPreferredProgrsamForInstanceId(anyString())).thenReturn(true);

        processor.setMappingObj(mappingJsonObj);

        ProcessedTableRow actual = processor.process(tableRowObject);

        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        ProcessedTableRow expected = getExpected(getEvent(""));
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
        tableRowObject.addProperty("enrollment_id", enrollmentId);
        tableRowObject.addProperty("uic", uic);
        tableRowObject.addProperty("Gender", gender);
        tableRowObject.addProperty("Mothers_First_Name", mothersFirstName);
        tableRowObject.addProperty("Date_of_Birth", dateOfBirth);
        tableRowObject.addProperty("Last_Name", lastName);
        tableRowObject.addProperty("District_of_Birth", districtOfBorth);
        tableRowObject.addProperty("Are_you_Twin", areYouTwin);

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
        tableRowObject.addProperty("enrollment_id", enrollmentId);
        tableRowObject.addProperty("uic", uic);
        tableRowObject.addProperty("Gender", gender);
        tableRowObject.addProperty("Mothers_First_Name", mothersFirstName);
        tableRowObject.addProperty("Date_of_Birth", dateOfBirth);
        tableRowObject.addProperty("Last_Name", lastName);
        tableRowObject.addProperty("District_of_Birth", districtOfBorth);
        tableRowObject.addProperty("Are_you_Twin", areYouTwin);

        return tableRowObject;
    }

    private JsonObject getMappingJsonObj() {
        JsonObject mappingJsonObj = new JsonObject();
        mappingJsonObj.addProperty("crptc", "gXNu7zJBTDN");
        mappingJsonObj.addProperty("date_created_of_event", "zJBTDNgXNu7");

        return mappingJsonObj;
    }

    private ProcessedTableRow getExpected(Event event) {
        return new ProcessedTableRow("1", getEnrollmentPayLoad(event));
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(Event event) {
        return new EnrollmentAPIPayLoad(
                enrollmentId,
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

    private Event getEvent(String eventId) {
        return new Event(
                    eventId,
                    instanceId,
                    enrollmentId,
                    program,
                    programStage,
                    orgUnitId,
                    eventDateValue,
                    status,
                    "1",
                    dataValues
            );
    }

    private List<TrackedEntityInstanceInfo> getTrackedEntityInstance() {
        List<TrackedEntityInstanceInfo> trackedEntityInstanceInfos = new LinkedList<>();
        List<Attribute> attributesOfPatient1 = new ArrayList<>();

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "***REMOVED***",
                "MMD_PER_NAM",
                "First name",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "w75KJ2mc4zz",
                "Michel"
        ));

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "***REMOVED***",
                "",
                "Last name",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "zDhUuAYrxNC",
                "Jackson"
        ));

        trackedEntityInstanceInfos.add(new TrackedEntityInstanceInfo(
                "2018-09-21T17:54:00.294",
                "SxgCPPeiq3c",
                "2018-09-21T17:54:01.337",
                "w3MoRtzP4SO",
                "2018-09-21T17:54:01.337",
                "o0kaqrZa79Y",
                "2018-09-21T17:54:01.337",
                false,
                false,
                "NONE",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                attributesOfPatient1
        ));

        return trackedEntityInstanceInfos;
    }
}
