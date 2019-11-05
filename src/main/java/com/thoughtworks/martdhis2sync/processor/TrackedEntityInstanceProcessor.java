package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceInfo;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    @Setter
    private List<String> comparableAttributes;


    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String process(Object tableRow) {

        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        getInstanceId(tableRowJsonObject, mappingJsonObject);
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
                                "\"trackedEntityInstance\": %s, " +
                                "\"orgUnit\":%s, \"attributes\":[",
                        teUID,
                        tableRowJsonObject.get("instance_id").toString(),
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

    private void getInstanceId(JsonObject tableRowJsonObject, JsonObject mappingJsonObject) {
        String instanceId = tableRowJsonObject.get("instance_id").getAsString();
        Map<String, String> searchableMappings = new HashMap<>();
        Map<String, String> comparableMappings = new HashMap<>();
        List<TrackedEntityInstanceInfo> matchedInstances;
        Set<String> searchableKeySet;
        Set<String> comparableKeySet;
        String uid;

        if (instanceId.isEmpty()) {
            searchableAttributes.forEach(searchableAttribute ->
                    searchableMappings.put(
                            mappingJsonObject.get(searchableAttribute).getAsString(),
                            tableRowJsonObject.get(searchableAttribute).getAsString()
                    )
            );

            comparableAttributes.forEach(comparableAttribute ->
                    comparableMappings.put(
                            mappingJsonObject.get(comparableAttribute).getAsString(),
                            tableRowJsonObject.get(comparableAttribute).getAsString()
                    )
            );


            searchableKeySet = searchableMappings.keySet();
            comparableKeySet = comparableMappings.keySet();

            matchedInstances = TEIUtil.getTrackedEntityInstanceInfos().stream().filter(trackedEntityInstance ->
                    trackedEntityInstance.getAttributes().stream().filter(attribute ->
                            searchableKeySet.contains(attribute.getAttribute())
                    ).allMatch(attribute ->
                            searchableMappings.get(attribute.getAttribute()).equals(attribute.getValue())
                    )
            ).filter(trackedEntityInstance ->
                    trackedEntityInstance.getAttributes().stream().filter(attribute ->
                            comparableKeySet.contains(attribute.getAttribute())
                    ).allMatch(attribute ->
                            comparableMappings.get(attribute.getAttribute()).equals(attribute.getValue())
                    )
            ).collect(Collectors.toList());

            if (matchedInstances.size() == 1) {
                uid = matchedInstances.get(0).getTrackedEntityInstance();
                tableRowJsonObject.addProperty("instance_id", uid);
                TEIUtil.setTrackedEntityInstanceIDs(tableRowJsonObject);
            }
        }
    }

    private String changeFormatIfDate(String attributeId, String value) {
        logger.info("TEI Processor : changeFormatIfDate: " + attributeId + ", " + value);
        if (TEIUtil.getAttributeOfTypeDate().contains(getUnquotedString(attributeId))) {
            String result = getQuotedString(BatchUtil.getDateOnly(value);
            logger.info("TEI Processor : getQuotedString(Date): " + result);
            return result;
        } else {
            if (TEIUtil.getAttributeOfTypeDateTime().contains(getUnquotedString(attributeId))) {
                String result = getQuotedString(BatchUtil.getDateTime(value));
                logger.info("TEI Processor : getQuotedString(DateTime): " + result);
                return result;
            }
        }

        return value;
    }
}
