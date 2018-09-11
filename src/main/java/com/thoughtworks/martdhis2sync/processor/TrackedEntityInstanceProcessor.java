package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

@Component
public class TrackedEntityInstanceProcessor implements ItemProcessor {

    private static final String EMPTY_STRING = "\"\"";
    private static final String ORGUNIT_UID = "orgunit_id";
    private static final int ONE = 1;

    @Value("${tracked.entity.type.person.uid}")
    private String teUID;

    @Setter
    private Object mappingObj;

    private Logger logger = LoggerFactory.getLogger(TrackedEntityInstanceProcessor.class);

    @Override
    public String process(Object tableRow) {

        Gson gson = new Gson();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        TEIUtil.setPatientIds(tableRowJsonObject);

        String dateCreated = tableRowJsonObject.get("date_created").toString();
        String substring = dateCreated.substring(ONE, dateCreated.length() - ONE);

        try {
            DateFormat simpleDateFormat = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss aa");
            Date bahmniDateCreated = simpleDateFormat.parse(substring);
            if (TEIUtil.date.compareTo(bahmniDateCreated) < 1) {
                TEIUtil.date = bahmniDateCreated;
            }
        } catch (ParseException e) {
            logger.error("TrackedEntityProcessor: " + e);
        }

        Set<String> keys = tableRowJsonObject.keySet();

        StringBuilder attributeSet = new StringBuilder(
                String.format("{\"trackedEntityType\": \"%s\", " +
                                "\"trackedEntityInstance\": %s, " +
                                "\"orgUnit\":%s, \"attributes\":[",
                        teUID, tableRowJsonObject.get("instance_id").toString(),
                        tableRowJsonObject.get(ORGUNIT_UID).toString()));
        for (String key : keys) {
            if (null != mappingJsonObject.get(key)) {
                String attribute = mappingJsonObject.get(key).toString();
                String value = tableRowJsonObject.get(key).toString();
                if (!EMPTY_STRING.equals(attribute)) {
                    attributeSet.append(String.format("{\"attribute\": %s, \"value\": %s},", attribute, value));
                }
            }
        }

        attributeSet.deleteCharAt(attributeSet.length() - 1);
        attributeSet.append("]}");

        return attributeSet.toString();
    }
}
