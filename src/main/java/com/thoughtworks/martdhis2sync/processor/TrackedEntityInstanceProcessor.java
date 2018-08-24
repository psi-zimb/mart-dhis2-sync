package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class TrackedEntityInstanceProcessor implements ItemProcessor {

    public static final String EMPTY_STRING = "\"\"";
    @Value("${tracked.entity.uid}")
    private String teUID;

    @Value("${org.unit.uid}")
    private String orgUnitUID;

    @Setter
    private Object mappingObj;

    @Override
    public String process(Object tableRow) {

        Gson gson = new Gson();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        Set<String> keys = tableRowJsonObject.keySet();

        StringBuilder attributeSet = new StringBuilder(String.format("{\"trackedEntityType\": \"%s\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"%s\"," +
                "\"attributes\":[", teUID, orgUnitUID));
        for(String key: keys) {
            String attribute = mappingJsonObject.get(key).toString();
            String value = tableRowJsonObject.get(key).toString();
            if (!EMPTY_STRING.equals(attribute)) {
                attributeSet.append(String.format("{\"attribute\": %s, \"value\": %s},", attribute, value));
            }
        }

        attributeSet.deleteCharAt(attributeSet.length() - 1);
        attributeSet.append("]}");

        return attributeSet.toString();
    }
}
