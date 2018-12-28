package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValueForStaticField;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class TEIUtilTest {

    @Test
    public void shouldAddPatientIdsToTheList() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("Patient_Identifier", "8ujk3jd");
        tableRowObject.addProperty("instance_id", "nLGUkAmW1YS");

        TEIUtil.setPatientIds(tableRowObject);

        assertEquals(1, TEIUtil.getPatientIdTEIUidMap().size());
        assertEquals("\"nLGUkAmW1YS\"", TEIUtil.getPatientIdTEIUidMap().get("\"8ujk3jd\""));
    }

    @Test
    public void shouldAddValueAsEmptyWhenTheInstanceIdIsNull() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("Patient_Identifier", "8ujk3jd");
        TEIUtil.setPatientIds(tableRowObject);

        assertEquals(1, TEIUtil.getPatientIdTEIUidMap().size());
        assertEquals("", TEIUtil.getPatientIdTEIUidMap().get("\"8ujk3jd\""));
    }

    @Test
    public void shouldClearTheList() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> patientTeiIds = new LinkedHashMap<>();
        patientTeiIds.put("key", "value");
        setValueForStaticField(TEIUtil.class, "patientIdTEIUidMap", patientTeiIds);

        assertEquals(1, TEIUtil.getPatientIdTEIUidMap().size());

        TEIUtil.resetPatientTEIUidMap();

        assertEquals(0, TEIUtil.getPatientIdTEIUidMap().size());
    }

    @Test
    public void shouldAddToTheTrackedEntityInstances() {
        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("Patient_Identifier", "NAH00001");
        tableRowObject.addProperty("instance_id", "88djSeH");

        TEIUtil.resetTrackedEntityInstaceIDs();

        TEIUtil.setTrackedEntityInstanceIDs(tableRowObject);

        assertEquals("88djSeH", TEIUtil.getTrackedEntityInstanceIDs().get("NAH00001"));
    }

    @Test
    public void shouldClearTrackedEntityInstanceIds() {
        TEIUtil.resetTrackedEntityInstaceIDs();

        JsonObject tableRowObject = new JsonObject();
        tableRowObject.addProperty("Patient_Identifier", "NAH00001");
        tableRowObject.addProperty("instance_id", "88djSeH");

        TEIUtil.setTrackedEntityInstanceIDs(tableRowObject);

        assertEquals(1, TEIUtil.getTrackedEntityInstanceIDs().size());

        TEIUtil.resetTrackedEntityInstaceIDs();

        assertEquals(0, TEIUtil.getTrackedEntityInstanceIDs().size());
    }
}
