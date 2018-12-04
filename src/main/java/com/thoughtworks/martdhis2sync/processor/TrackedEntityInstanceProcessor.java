package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstance;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;

@Component
public class TrackedEntityInstanceProcessor implements ItemProcessor {

    private static final String EMPTY_STRING = "\"\"";
    private static final String ORGUNIT_UID = "orgunit_id";

    @Value("${tracked.entity.type.person.uid}")
    private String teUID;

    @Setter
    private Object mappingObj;

    @Setter
    private List<String> searchableAttributes;

    @Override
    public String process(Object tableRow) {

        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        TEIUtil.setPatientIds(tableRowJsonObject);
        updateLatestDateCreated(tableRowJsonObject.get("date_created").toString());

        return createRequestBodyForTrackedEntityInstance(tableRowJsonObject, mappingJsonObject);
    }

    private void updateLatestDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(getUnquotedString(dateCreated), DATEFORMAT_WITH_24HR_TIME);
        if (TEIUtil.date.compareTo(bahmniDateCreated) < 1) {
            TEIUtil.date = bahmniDateCreated;
        }
    }

    private String createRequestBodyForTrackedEntityInstance(JsonObject tableRowJsonObject, JsonObject mappingJsonObject) {
        Set<String> keys = tableRowJsonObject.keySet();

        StringBuilder attributeSet = new StringBuilder(
                String.format("{\"trackedEntityType\": \"%s\", " +
                                "\"trackedEntityInstance\": \"%s\", " +
                                "\"orgUnit\":%s, \"attributes\":[",
                        teUID,
                        getInstanceId(tableRowJsonObject, mappingJsonObject),
                        tableRowJsonObject.get(ORGUNIT_UID).toString()));
        for (String key : keys) {
            if (null != mappingJsonObject.get(key)) {
                String attribute = mappingJsonObject.get(key).toString();
                String value = tableRowJsonObject.get(key).toString();
                if (!EMPTY_STRING.equals(attribute)) {
                    attributeSet.append(String.format(
                            "{\"attribute\": %s, \"value\": %s},",
                            attribute,
                            changeFormatIfDate(attribute, value)
                    ));
                }
            }
        }
        attributeSet.deleteCharAt(attributeSet.length() - 1);
        attributeSet.append("]}");
        return attributeSet.toString();
    }

    private String getInstanceId(JsonObject tableRowJsonObject, JsonObject mappingJsonObject) {
        String instanceId = tableRowJsonObject.get("instance_id").getAsString();
        Map<String, String> searchableMappings = new HashMap<>();

        if (instanceId.isEmpty()) {
            searchableAttributes.forEach(s ->
                    searchableMappings.put(mappingJsonObject.get(s).getAsString(), tableRowJsonObject.get(s).getAsString())
            );

            Set<String> searchableKeySet = searchableMappings.keySet();

            List<TrackedEntityInstance> matchedInstances = TEIUtil.getTrackedEntityInstances().stream().filter(trackedEntityInstance ->
                    trackedEntityInstance.getAttributes().stream().filter(attribute ->
                            searchableKeySet.contains(attribute.getAttribute())
                    ).allMatch(attribute ->
                            searchableMappings.get(attribute.getAttribute()).equals(attribute.getValue())
                    )
            ).collect(Collectors.toList());

            if (matchedInstances.size() == 1) {
                return matchedInstances.get(0).getTrackedEntityInstance();
            }

            return "";
        }

        return instanceId;
    }

    private String changeFormatIfDate(String attributeId, String value) {
        return TEIUtil.getAttributeOfTypeDateTime().contains(getUnquotedString(attributeId)) ?
                getQuotedString(
                        BatchUtil.getFormattedDateString(
                                getUnquotedString(value),
                                DATEFORMAT_WITH_24HR_TIME,
                                DHIS_ACCEPTABLE_DATEFORMAT
                        )
                ) :
                value;
    }
}
