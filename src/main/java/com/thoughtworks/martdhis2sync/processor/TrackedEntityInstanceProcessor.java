package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentDetails;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceInfo;
import com.thoughtworks.martdhis2sync.service.EnrollmentService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Value("${tracked.entity.preferred.program}")
    private String preferredProgramToAutoEnroll;

    @Setter
    private Object mappingObj;

    @Setter
    private List<String> searchableAttributes;

    @Setter
    private List<String> comparableAttributes;

    @Autowired
    EnrollmentService enrollmentService;

    @Autowired
    private TEIService teiService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private LoggerService loggerService;

    @Override
    public String process(Object tableRow) throws Exception {

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

    private String createRequestBodyForTrackedEntityInstance(JsonObject tableRowJsonObject, JsonObject mappingJsonObject) throws Exception {
        Set<String> keys = tableRowJsonObject.keySet();
        String uic = tableRowJsonObject.get("UIC").getAsString();
        List<TrackedEntityInstanceInfo> trackedEntityInstances = teiService.getTrackedEntityInstancesForUIC(uic);
        if (trackedEntityInstances.size() == 0) {
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
        } else if (trackedEntityInstances.size() == 1){
            loggerService.collateLogMessage("Encountered already existing instance in DHIS with UIC : " + uic);
            TrackedEntityInstanceInfo teiInfo = trackedEntityInstances.get(0);
            if(teiService.instanceExistsInDHIS(tableRowJsonObject,Collections.singletonList(teiInfo))){
                List<EnrollmentDetails> enrollmentsToPreferredProgram = teiInfo.getEnrollments().stream().filter(enrollmentDetails -> enrollmentDetails.getProgram().equals(preferredProgramToAutoEnroll)).collect(Collectors.toList());
                if(enrollmentsToPreferredProgram.size()==0){
                    enrollmentService.enrollSingleClientInstanceToPreferredProgram(teiInfo);
                    loggerService.collateLogMessage("Enrolling one instance into preferred program."+preferredProgramToAutoEnroll);
                }
                return "";
            }
            else{
                loggerService.collateLogMessage("Found different client in DHIS with same UIC.Cant proceed creating new client. Skipping client record. UIC :" + uic);
            }
        }
        return "";
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
                            searchableMappings.get(attribute.getAttribute()).equalsIgnoreCase(attribute.getValue())
                    )
            ).filter(trackedEntityInstance ->
                    trackedEntityInstance.getAttributes().stream().filter(attribute ->
                            comparableKeySet.contains(attribute.getAttribute())
                    ).allMatch(attribute ->
                            comparableMappings.get(attribute.getAttribute()).equalsIgnoreCase(attribute.getValue())
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
        logger.debug("TEI Processor : changeFormatIfDate: " + attributeId + ", " + value);
        if (TEIUtil.getAttributeOfTypeDate() != null && TEIUtil.getAttributeOfTypeDate().contains(getUnquotedString(attributeId))) {
            String result = getQuotedString(BatchUtil.getDateOnly(getUnquotedString(value)));
            logger.debug("TEI Processor : getQuotedString(Date): " + result);
            return result;
        } else {
            if (TEIUtil.getAttributeOfTypeDateTime() != null && TEIUtil.getAttributeOfTypeDateTime().contains(getUnquotedString(attributeId))) {
                String result = getQuotedString(
                        BatchUtil.getFormattedDateString(
                                getUnquotedString(value),
                                DATEFORMAT_WITH_24HR_TIME,
                                DHIS_ACCEPTABLE_DATEFORMAT
                        )
                );
                logger.debug("TEI Processor : getQuotedString(DateTime): " + result);
                return result;
            }
        }

        return value;
    }
}
