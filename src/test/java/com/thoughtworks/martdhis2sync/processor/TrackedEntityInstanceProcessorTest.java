package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Attribute;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstance;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TEIUtil.class, BatchUtil.class})
public class TrackedEntityInstanceProcessorTest {
    @Mock
    private Date teiDate;

    private TrackedEntityInstanceProcessor processor;
    private Date bahmniDate = new Date(Long.MIN_VALUE);
    private String dateCreated = "2018-02-02 13:46:23";
    private List<String> dateTimeAttributes = new LinkedList();
    private String dhisAcceptableDate = "2018-02-02T13:46:23";

    @Before
    public void setUp() throws Exception {
        processor = new TrackedEntityInstanceProcessor();
        setValuesForMemberFields(processor, "teUID", "o0kaqrZaY");
        dateTimeAttributes.add("aQLSyCrOb34");

        mockStatic(TEIUtil.class);
        doNothing().when(TEIUtil.class);
        TEIUtil.setPatientIds(getTableRowObject());
        TEIUtil.setTrackedEntityInstances(getTrackedEntityInstances());
        processor.setSearchableAttributes(Collections.singletonList("UIC"));
        processor.setComparableAttributes(Arrays.asList("patient_id", "prepID"));
        TEIUtil.date = teiDate;

        mockStatic(BatchUtil.class);
        when(getUnquotedString("\"" + dateCreated + "\"")).thenReturn(dateCreated);
        when(getUnquotedString("\"aQLSyCrOb34\"")).thenReturn("aQLSyCrOb34");
        when(getUnquotedString("\"rOb34aQLSyC\"")).thenReturn("rOb34aQLSyC");
        when(getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME)).thenReturn(bahmniDate);
        when(getFormattedDateString(dateCreated, DATEFORMAT_WITH_24HR_TIME, DHIS_ACCEPTABLE_DATEFORMAT)).thenReturn(dhisAcceptableDate);
        when(getQuotedString(dhisAcceptableDate)).thenReturn("\"" + dhisAcceptableDate + "\"");
        when(TEIUtil.getAttributeOfTypeDateTime()).thenReturn(dateTimeAttributes);
    }

    @Test
    public void shouldReturnTeiRequestBodyForAPatientAndShouldUpdateTEIUtilDateIfTheDateCreatedOfTheRecordIsGreater() {
        when(teiDate.compareTo(bahmniDate)).thenReturn(0);

        processor.setMappingObj(getMappingJsonObj());
        String actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(bahmniDate, TEIUtil.date);
    }

    @Test
    public void shouldReturnTeiRequestBodyForAPatientAndShouldNotUpdateTEIUtilDateIfTheDateCreatedOfTheRecordIsLesser() {
        when(teiDate.compareTo(bahmniDate)).thenReturn(1);

        processor.setMappingObj(getMappingJsonObj());
        String actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(teiDate, TEIUtil.date);
    }

    @Test
    public void shouldNotIncludeTheColumnInTheAttributesOfTheRequestBodyIfTheMappingIsEmptyForThatColumn() {
        when(teiDate.compareTo(bahmniDate)).thenReturn(1);

        JsonObject mappingJsonObj = getMappingJsonObj();
        mappingJsonObj.addProperty("Patient_Identifier", "");
        processor.setMappingObj(mappingJsonObj);
        String actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(teiDate, TEIUtil.date);
    }

    @Test
    public void shouldAddTheTrackedEntityInstanceIdToRequestBodyIfPatientAlreadyCreatedInDHISAndBahmniDidNotHaveAnyTEI() {
        when(TEIUtil.getTrackedEntityInstances()).thenReturn(getTrackedEntityInstances());

        JsonObject tableRowObject = getTableRowObject();
        tableRowObject.addProperty("instance_id", "");
        tableRowObject.addProperty("UIC", "UIC00015");
        tableRowObject.addProperty("prepID", "0097");

        processor.setMappingObj(getMappingJsonObj());

        String actual = processor.process(tableRowObject);

        tableRowObject.addProperty("instance_id", "w3MoRtzP4SO");

        verifyStatic();
        TEIUtil.setPatientIds(tableRowObject);
        verifyStatic(times(2));
        getUnquotedString("\"" + dateCreated + "\"");
        verifyStatic();
        getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(4));
        TEIUtil.getAttributeOfTypeDateTime();
        verifyStatic(times(1));
        BatchUtil.getQuotedString(dhisAcceptableDate);
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"rOb34aQLSyC\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"aQLSyCrOb34\"");
        verifyStatic(times(2));
        BatchUtil.getUnquotedString("\"" + dateCreated + "\"");

        assertEquals(getExpectedBodyInCaseDHISAlreadyHaveCurrentPatient(), actual);
    }

    @Test
    public void shouldNotAddAnyTrackedEntityInstanceToTheRequestBodyIfPatientIsNotCreatedInDHIS() {
        List<TrackedEntityInstance> trackedEntityInstances = getTrackedEntityInstances();
        trackedEntityInstances.get(0).getAttributes().get(0).setValue("UIC00014");
        when(TEIUtil.getTrackedEntityInstances()).thenReturn(trackedEntityInstances);

        JsonObject tableRowObject = getTableRowObject();
        tableRowObject.addProperty("instance_id", "");
        tableRowObject.addProperty("prep_ID", "0097");
        tableRowObject.addProperty("UIC", "UIC00015");
        tableRowObject.addProperty("patient_id", "NAH0000321");

        processor.setMappingObj(getMappingJsonObj());

        String actual = processor.process(tableRowObject);

        verifyStatic();
        TEIUtil.setPatientIds(tableRowObject);
        verifyStatic(times(2));
        getUnquotedString("\"" + dateCreated + "\"");
        verifyStatic();
        getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(4));
        TEIUtil.getAttributeOfTypeDateTime();
        verifyStatic(times(1));
        BatchUtil.getQuotedString(dhisAcceptableDate);
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"rOb34aQLSyC\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"aQLSyCrOb34\"");
        verifyStatic(times(2));
        BatchUtil.getUnquotedString("\"" + dateCreated + "\"");

        assertEquals(getExpectedBodyInCaseDHISDoesNotHaveCurrentPatient(), actual);
    }

    @Test
    public void shouldNotAddAnyTrackedEntityInstanceToRequestBodyIfPatientSearchableAttributeIsMatchingButComparableAttributesAreNot() {
        when(TEIUtil.getTrackedEntityInstances()).thenReturn(getTrackedEntityInstances());

        JsonObject tableRowObject = getTableRowObject();
        tableRowObject.addProperty("instance_id", "");
        tableRowObject.addProperty("patient_id", "NAH0000321");
        tableRowObject.addProperty("UIC", "UIC00015");
        tableRowObject.addProperty("prepId", "123");

        processor.setMappingObj(getMappingJsonObj());

        String actual = processor.process(tableRowObject);

        verifyStatic();
        TEIUtil.setPatientIds(tableRowObject);
        verifyStatic(times(2));
        getUnquotedString("\"" + dateCreated + "\"");
        verifyStatic();
        getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(4));
        TEIUtil.getAttributeOfTypeDateTime();
        verifyStatic(times(1));
        BatchUtil.getQuotedString(dhisAcceptableDate);
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"rOb34aQLSyC\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"aQLSyCrOb34\"");
        verifyStatic(times(2));
        BatchUtil.getUnquotedString("\"" + dateCreated + "\"");

        assertEquals(getExpectedBodyInCaseDHISDoesNotHaveCurrentPatient(), actual);
    }

    private void mockVerify() {
        verifyStatic();
        TEIUtil.setPatientIds(getTableRowObject());
        verifyStatic(times(2));
        getUnquotedString("\"" + dateCreated + "\"");
        verifyStatic();
        getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(4));
        TEIUtil.getAttributeOfTypeDateTime();
        verifyStatic(times(1));
        BatchUtil.getQuotedString(dhisAcceptableDate);
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"rOb34aQLSyC\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\"aQLSyCrOb34\"");
        verifyStatic(times(2));
        BatchUtil.getUnquotedString("\"" + dateCreated + "\"");
    }

    private JsonObject getMappingJsonObj() {
        JsonObject mappingJsonObj = new JsonObject();
        mappingJsonObj.addProperty("UIC", "rOb34aQLSyC");
        mappingJsonObj.addProperty("patient_id", "4aQLSyCrOb3");
        mappingJsonObj.addProperty("prepID", "SyCrOb34aQL");
        mappingJsonObj.addProperty("date_created", "aQLSyCrOb34");

        return mappingJsonObj;
    }

    private List<TrackedEntityInstance> getTrackedEntityInstances() {
        LinkedList<TrackedEntityInstance> trackedEntityInstances = new LinkedList<>();
        List<Attribute> attributesOfPatient1 = new LinkedList<>();

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "admin",
                "MMD_PER_NAM",
                "UIC",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "rOb34aQLSyC",
                "UIC00015"
        ));

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "admin",
                "",
                "First name",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "zDhUuAYrxNC",
                "Jackson"
        ));

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "admin",
                "MMD_PER_NAM",
                "prepID",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "SyCrOb34aQL",
                "0097"
        ));

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "admin",
                "",
                "patient_id",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "4aQLSyCrOb3",
                "NAH0000123"
        ));

        trackedEntityInstances.add(new TrackedEntityInstance(
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

        return trackedEntityInstances;
    }

    private JsonObject getTableRowObject() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("Patient_Identifier", "UIC00014");
        tableRowObject.addProperty("OrgUnit", "PSI-ZIMB-NAH");
        tableRowObject.addProperty("UIC", "UIC00014");
        tableRowObject.addProperty("prepID", "00970");
        tableRowObject.addProperty("patient_id", "NAH0000123");
        tableRowObject.addProperty("date_created", dateCreated);
        tableRowObject.addProperty("instance_id", "EmACSYDCxhu");
        tableRowObject.addProperty("orgunit_id", "SxgCPPeiq3c");

        return tableRowObject;
    }

    private String getExpected() {
        return "{" +
                "\"trackedEntityType\": \"o0kaqrZaY\", " +
                "\"trackedEntityInstance\": \"EmACSYDCxhu\", " +
                "\"orgUnit\":\"SxgCPPeiq3c\", " +
                "\"attributes\":" +
                "[" +
                "{" +
                "\"attribute\": \"rOb34aQLSyC\", " +
                "\"value\": \"UIC00014\"" +
                "}," +
                "{" +
                "\"attribute\": \"SyCrOb34aQL\", " +
                "\"value\": \"00970\"" +
                "},{" +
                "\"attribute\": \"4aQLSyCrOb3\", " +
                "\"value\": \"NAH0000123\"" +
                "}," +
                "{" +
                "\"attribute\": \"aQLSyCrOb34\", " +
                "\"value\": \"" + dhisAcceptableDate + "\"" +
                "}" +
                "]" +
                "}";
    }

    private String getExpectedBodyInCaseDHISAlreadyHaveCurrentPatient() {
        return "{" +
                "\"trackedEntityType\": \"o0kaqrZaY\", " +
                "\"trackedEntityInstance\": \"w3MoRtzP4SO\", " +
                "\"orgUnit\":\"SxgCPPeiq3c\", " +
                "\"attributes\":" +
                "[" +
                "{" +
                "\"attribute\": \"rOb34aQLSyC\", " +
                "\"value\": \"UIC00015\"" +
                "}," +
                "{" +
                "\"attribute\": \"SyCrOb34aQL\", " +
                "\"value\": \"0097\"" +
                "},{" +
                "\"attribute\": \"4aQLSyCrOb3\", " +
                "\"value\": \"NAH0000123\"" +
                "}," +
                "{" +
                "\"attribute\": \"aQLSyCrOb34\", " +
                "\"value\": \"" + dhisAcceptableDate + "\"" +
                "}" +
                "]" +
                "}";
    }

    private String getExpectedBodyInCaseDHISDoesNotHaveCurrentPatient() {
        return "{" +
                "\"trackedEntityType\": \"o0kaqrZaY\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"SxgCPPeiq3c\", " +
                "\"attributes\":" +
                "[" +
                "{" +
                "\"attribute\": \"rOb34aQLSyC\", " +
                "\"value\": \"UIC00015\"" +
                "}," +
                "{" +
                "\"attribute\": \"SyCrOb34aQL\", " +
                "\"value\": \"00970\"" +
                "},{" +
                "\"attribute\": \"4aQLSyCrOb3\", " +
                "\"value\": \"NAH0000321\"" +
                "}," +
                "{" +
                "\"attribute\": \"aQLSyCrOb34\", " +
                "\"value\": \"" + dhisAcceptableDate + "\"" +
                "}" +
                "]" +
                "}";
    }
}
