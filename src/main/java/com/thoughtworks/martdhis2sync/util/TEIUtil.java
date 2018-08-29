package com.thoughtworks.martdhis2sync.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;

public class TEIUtil {

    @Getter
    private static Map<String, String> patientIdTEIUidMap = new LinkedHashMap<>();

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
}
