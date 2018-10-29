package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Enrollment;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EnrollmentUtil.class, BatchUtil.class})
public class ProgramEnrollmentProcessorTest {
    @Mock
    private Date enrollmentDate;

    private ProgramEnrollmentProcessor processor;
    private Date bahmniDate = new Date(Long.MIN_VALUE);
    private String dateCreated = "2018-10-25 13:46:23";
    private String instanceId = "EmACSYDCxhu";
    private String program = "SxgCPPeiq3c";
    private String orgUnit = "ofaUrIe32";
    private String status = "ACTIVE";

    @Before
    public void setUp() throws Exception {
        String enrollmentDate = "2018-10-25";
        processor = new ProgramEnrollmentProcessor();

        mockStatic(EnrollmentUtil.class);
        EnrollmentUtil.date = this.enrollmentDate;

        mockStatic(BatchUtil.class);
        when(BatchUtil.getUnquotedString("\""+ dateCreated +"\"")).thenReturn(dateCreated);
        when(BatchUtil.getUnquotedString("\"\"")).thenReturn("");
        when(BatchUtil.getUnquotedString("\""+ instanceId +"\"")).thenReturn(instanceId);
        when(BatchUtil.getUnquotedString("\""+ program +"\"")).thenReturn(program);
        when(BatchUtil.getUnquotedString("\""+ orgUnit +"\"")).thenReturn(orgUnit);
        when(BatchUtil.getUnquotedString("\""+ status +"\"")).thenReturn(status);
        when(BatchUtil.getDateFromString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME)).thenReturn(bahmniDate);
        when(BatchUtil.getFormattedDateString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME, BatchUtil.DATEFORMAT_WITHOUT_TIME)).thenReturn(enrollmentDate);
    }

    @Test
    public void shouldReturnEnrollmentRequestBodyForAPatientAndShouldUpdateEnrollmentUtilDateIfTheDateCreatedOfTheRecordIsGreater() {
        when(enrollmentDate.compareTo(bahmniDate)).thenReturn(0);

        Enrollment actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(bahmniDate, EnrollmentUtil.date);
    }

    @Test
    public void shouldReturnEnrollmentRequestBodyForAPatientAndShouldNotUpdateEnrollmentUtilDateIfTheDateCreatedOfTheRecordIsLesser() {
        when(enrollmentDate.compareTo(bahmniDate)).thenReturn(1);

        Enrollment actual = processor.process(getTableRowObject());

        mockVerify();

        assertEquals(getExpected(), actual);
        assertEquals(enrollmentDate, EnrollmentUtil.date);
    }

    private void mockVerify() {
        verifyStatic();
        BatchUtil.getUnquotedString("\"\"");
        verifyStatic(times(3));
        BatchUtil.getUnquotedString("\""+ dateCreated +"\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\""+ instanceId +"\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\""+ program +"\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\""+ orgUnit +"\"");
        verifyStatic(times(1));
        BatchUtil.getUnquotedString("\""+ status +"\"");
        verifyStatic(times(1));
        BatchUtil.getDateFromString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        verifyStatic(times(2));
        BatchUtil.getFormattedDateString(dateCreated, BatchUtil.DATEFORMAT_WITH_24HR_TIME, BatchUtil.DATEFORMAT_WITHOUT_TIME);
    }

    private JsonObject getTableRowObject() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("instance_id", instanceId);
        tableRowObject.addProperty("program", program);
        tableRowObject.addProperty("orgunit_id", orgUnit);
        tableRowObject.addProperty("enrollment_date", dateCreated);
        tableRowObject.addProperty("incident_date", dateCreated);
        tableRowObject.addProperty("status", status);
        tableRowObject.addProperty("program_unique_id", "v3");
        tableRowObject.addProperty("date_created", dateCreated);
        tableRowObject.addProperty("enrollment_id", "");

        return tableRowObject;
    }

    private Enrollment getExpected() {
        return new Enrollment("", "EmACSYDCxhu", "SxgCPPeiq3c", "ofaUrIe32", "2018-10-25", "2018-10-25", "ACTIVE", "\"v3\"");
    }
}
