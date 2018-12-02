package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.ParseException;
import java.util.LinkedList;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BatchUtil.class})
public class NewCompletedEnrollmentProcessorTest {
    private NewCompletedEnrollmentProcessor processor;

    private String enrDate = "2018-10-24";
    private String status = "COMPLETED";
    private String instanceId = "EmACSYDCxhu";
    private String program = "SxgCPPeiq3c";
    private String orgUnitId = "ofaUrIe32";

    @Before
    public void setUp() throws Exception {
        processor = new NewCompletedEnrollmentProcessor();

        mockStatic(BatchUtil.class);
        when(getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME)).thenReturn(enrDate);
    }

    @Test
    public void shouldReturnEnrollmentApiRequestBodyForAPatient() throws ParseException {
        EnrollmentAPIPayLoad expected = getExpected();
        EnrollmentAPIPayLoad actual = processor.process(getTableRowObject());

        assertEquals(expected, actual);
        verifyStatic(times(2));
        getFormattedDateString(enrDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

    }

    private JsonObject getTableRowObject() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("\"Patient_Identifier\"", "NAH0001");
        tableRowObject.addProperty("program", program);
        tableRowObject.addProperty("enrollment_date", enrDate);
        tableRowObject.addProperty("incident_date", enrDate);
        tableRowObject.addProperty("status", status);
        tableRowObject.addProperty("date_created", "2018-10-24 13:46:23");
        tableRowObject.addProperty("program_unique_id", "1");
        tableRowObject.addProperty("\"OrgUnit\"", "PSI-ZIMB-NAH");
        tableRowObject.addProperty("orgunit_id", orgUnitId);
        tableRowObject.addProperty("instance_id", instanceId);

        return tableRowObject;
    }

    private EnrollmentAPIPayLoad getExpected() {

        return new EnrollmentAPIPayLoad(
                "",
                instanceId,
                program,
                orgUnitId,
                enrDate,
                enrDate,
                status,
                "1",
                new LinkedList<>()
        );
    }
}
