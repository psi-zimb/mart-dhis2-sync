package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstance;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TEIUtil {

    @Getter
    private static Map<String, String> patientIdTEIUidMap = new LinkedHashMap<>();

    @Getter
    @Setter
    private static List<String> attributeOfTypeDateTime;

    @Getter
    @Setter
    private static List<TrackedEntityInstance> trackedEntityInstances;

    @Getter
    private static Map<String, String> trackedEntityInstanceIDs = new LinkedHashMap<>();

    public static Date date = new Date(Long.MIN_VALUE);

    private static String jsonToString(JsonElement jsonElement) {
        if (null == jsonElement) {
            return "";
        }
        return jsonElement.toString();
    }

    public static void setPatientIds(JsonObject tableRowJsonObject) {
        patientIdTEIUidMap.put(
                jsonToString(tableRowJsonObject.get("Patient_Identifier")),
                jsonToString(tableRowJsonObject.get("instance_id"))
        );
    }

    public static void resetPatientTEIUidMap() {
        patientIdTEIUidMap.clear();
    }

    public static void resetTrackedEntityInstaceIDs() {
        trackedEntityInstanceIDs.clear();
    }

    public static void setTrackedEntityInstanceIDs(JsonObject tableRowJsonObject) {
        trackedEntityInstanceIDs.put(
                tableRowJsonObject.get("Patient_Identifier").getAsString(),
                tableRowJsonObject.get("instance_id").getAsString()
        );
    }
}
