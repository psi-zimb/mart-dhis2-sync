package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class TrackedEntityInstanceProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Override
    public String process(Object tableRow) throws Exception {

        Gson gson = new Gson();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        Set<String> keys = tableRowJsonObject.keySet();

        StringBuilder attributeSet = new StringBuilder(String.format("{\"trackedEntity\": \"%s\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"%s\"," +
                "\"attributes\":[", "trackedEntity", "orgUnit"));
        for(String key: keys) {
            String attribute = mappingJsonObject.get(key).toString();
            String value = tableRowJsonObject.get(key).toString();

            attributeSet.append(String.format("{\"attribute\": %s, \"value\": %s},", attribute, value));
        }
        if(attributeSet.length() > 0) {
            attributeSet.deleteCharAt(attributeSet.length() - 1);
        }

        attributeSet.append("]}");

        return attributeSet.toString();

    }
}
