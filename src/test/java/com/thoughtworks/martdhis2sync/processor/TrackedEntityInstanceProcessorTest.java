package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

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

    private void mockVerify() {
        verifyStatic();
        TEIUtil.setPatientIds(getTableRowObject());
        verifyStatic(times(2));
        getUnquotedString("\"" + dateCreated + "\"");
        verifyStatic();
        getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(2));
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
        mappingJsonObj.addProperty("date_created", "aQLSyCrOb34");

        return mappingJsonObj;
    }

    private JsonObject getTableRowObject() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("Patient_Identifier", "UIC00014");
        tableRowObject.addProperty("OrgUnit", "PSI-ZIMB-NAH");
        tableRowObject.addProperty("UIC", "UIC00014");
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
                "\"attribute\": \"aQLSyCrOb34\", " +
                "\"value\": \"" + dhisAcceptableDate + "\"" +
                "}" +
                "]" +
                "}";
    }
}
