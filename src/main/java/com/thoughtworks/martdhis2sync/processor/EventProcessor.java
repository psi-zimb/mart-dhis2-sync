package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Set;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;

@Component
public class EventProcessor implements ItemProcessor {

    public static final String EVENT_API_FORMAT = "{\"event\": \"\", " +
            "\"trackedEntityInstance\": %s, " +
            "\"enrollment\": %s, " +
            "\"program\": %s, " +
            "\"programStage\": %s, " +
            "\"orgUnit\":%s, " +
            "\"dataValues\":[%s]}";
    @Setter
    private Object mappingObj;

    @Override
    public Object process(Object tableRow) {
        Gson gson = new Gson();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        return createRequestBodyElements(tableRowJsonObject, mappingJsonObject);
    }

    private String createRequestBodyElements(JsonObject tableRow, JsonObject mapping) {

        return String.format(EVENT_API_FORMAT,
                tableRow.get("instance_id").toString(),
                tableRow.get("enrollment_id").toString(),
                tableRow.get("program").toString(),
                tableRow.get("program_stage").toString(),
                tableRow.get("orgunit_id").toString(),
                getDataValues(tableRow, mapping)
        );
    }

    private String getDataValues(JsonObject tableRow, JsonObject mapping) {
        Set<String> keys = tableRow.keySet();
        StringBuilder dataValues = new StringBuilder();

        for (String key : keys) {
            JsonElement dataElement = mapping.get(key);
            if (hasValue(dataElement)) {
                String value = tableRow.get(key).toString();
                dataValues.append(String.format("{\"dataElement\": %s, \"value\": %s},", dataElement.toString(), value));
            }
        }

        return removeLastChar(dataValues);
    }
}
