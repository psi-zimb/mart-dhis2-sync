package com.thoughtworks.martdhis2sync.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.internal.LinkedTreeMap;
import com.thoughtworks.martdhis2sync.dao.MappingDAO;
import com.thoughtworks.martdhis2sync.dao.PatientDAO;
import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.SyncFailedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.thoughtworks.martdhis2sync.util.MarkerUtil.*;

@Component
public class TEIService {
    private static final String TEI_URI = "/api/trackedEntityInstances?pageSize=10000";
    private static final String LOG_PREFIX = "TEI Service: ";
    private static final String TEI_JOB_NAME = "Sync Tracked Entity Instance";
    private final String TEI_ENROLLMENTS_URI = "/api/trackedEntityInstances?" +
            "fields=trackedEntityInstance,enrollments[program,enrollment,enrollmentDate,completedDate,status,events]&" +
            "program=%s&trackedEntityInstance=%s";
    private final String RECORDS_WITH_INVALID_ORG_UNIT_QUERY =
            "select \"Patient_Identifier\", \"OrgUnit\" from %s it " +
                    "where (\"OrgUnit\" is null or \"OrgUnit\" not in (select orgunit from orgunit_tracker ot))" +
                    " and date_created::TIMESTAMP > " +
                    "COALESCE((SELECT last_synced_date FROM marker WHERE category='%s' AND program_name='%s'), '-infinity');";

    @Value("${country.org.unit.id.for.patient.data.duplication.check}")
    private String orgUnitID;
    @Value("${tracked.entity.filter.uri.limit}")
    private int TEI_FILTER_URI_LIMIT;
    @Value("${tracked.entity.attribute.uic}")
    private String uicAttributeId;
    @Value("${tracked.entity.attribute.mothersfirstname}")
    private String mothersFirstNameAttributeId;
    @Value("${tracked.entity.attribute.gender}")
    private String genderAttributeId;
    @Value("${tracked.entity.attribute.dateofbirth}")
    private String dateOfBirthAttributeId;
    @Value("${tracked.entity.attribute.districtofbirth}")
    private String districtOfBirthAttributeId;
    @Value("${tracked.entity.attribute.lastname}")
    private String lastNameAttributeId;
    @Value("${tracked.entity.attribute.twin}")
    private String twinAttributeId;
    @Autowired
    private MappingDAO mappingDAO;
    @Autowired
    private TrackedEntityInstanceStep trackedEntityInstanceStep;
    @Autowired
    private JobService jobService;
    @Autowired
    private PatientDAO patientDAO;
    @Autowired
    private SyncRepository syncRepository;
    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    private Logger logger = LoggerFactory.getLogger(this.getClass());


    public void triggerJob(String service, String user, String lookupTable, Object mappingObj, List<String> searchableAttributes, List<String> comparableAttributes,String startDate, String endDate)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        try {
            LinkedList<Step> steps = new LinkedList<>();
            steps.add(trackedEntityInstanceStep.get(lookupTable, service, mappingObj, searchableAttributes, comparableAttributes, startDate, endDate));
            jobService.triggerJob(service, user, TEI_JOB_NAME, steps, "");
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }


    public void getTrackedEntityInstances(String mappingName, MappingJson mappingJson) throws Exception {
        List<TrackedEntityInstanceInfo> allTEIInfos = new ArrayList<>();
        StringBuilder url = new StringBuilder();

        url.append(TEI_URI);
        url.append("&ou=");
        url.append(orgUnitID);
        url.append("&ouMode=DESCENDANTS");

        Gson gson = new Gson();
        LinkedTreeMap instanceMapping = gson.fromJson(mappingJson.getInstance().toString(), LinkedTreeMap.class);

        List<Map<String, Object>> searchableFields = mappingDAO.getSearchableFieldsValues(mappingName);

        if (searchableFields.isEmpty()) {
            TEIUtil.setTrackedEntityInstanceInfos(Collections.emptyList());
            return;
        }

        for (List<Map<String, Object>> searchableFieldGroup : separateSearchFieldsBasedOnFilterLimit(searchableFields)) {
            StringBuilder uri = new StringBuilder();
            searchableFieldGroup.get(0).keySet().forEach(filter -> {
                uri.append("&filter=");
                uri.append(instanceMapping.get(filter));
                uri.append(":IN:");

                searchableFieldGroup.forEach(searchableField -> {
                    uri.append(searchableField.get(filter));
                    uri.append(";");
                });
            });
            uri.append("&includeAllAttributes=true");
            ResponseEntity<TrackedEntityInstanceResponse> response = syncRepository.getTrackedEntityInstances(url.toString() + uri);
            List<TrackedEntityInstanceInfo> trackedEntityInstanceInfos;
            if (response != null && response.getBody() != null) {
                trackedEntityInstanceInfos = response.getBody().getTrackedEntityInstances();
                if (trackedEntityInstanceInfos.size() > searchableFieldGroup.size()) {
                    logger.info("Found possible duplicates:" + trackedEntityInstanceInfos.size() + " entries in DHIS for "+searchableFieldGroup.size()+" searchable values");
                    allTEIInfos.addAll(getUniqueInstanceSet(trackedEntityInstanceInfos, mappingName));
                } else {
                    allTEIInfos.addAll(trackedEntityInstanceInfos);
                }
            }
        }

        TEIUtil.setTrackedEntityInstanceInfos(allTEIInfos);
        logger.info("TEIUtil.getTrackedEntityInstanceInfos().size(): " + TEIUtil.getTrackedEntityInstanceInfos().size());
    }

    private Collection<? extends TrackedEntityInstanceInfo> getUniqueInstanceSet(List<TrackedEntityInstanceInfo> trackedEntityInstanceInfos, String mappingName) throws IOException {
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
        List<TrackedEntityInstanceInfo> trackedEntityInstanceInfoList = new ArrayList<>();
        List<Map<String, Object>> instanceFieldsForAllLocalInstances = mappingDAO.getInstanceFieldsValues(mappingName);
        instanceFieldsForAllLocalInstances.stream().forEach(localInstance -> {
            JsonObject localInstanceJSON = gson.toJsonTree(localInstance).getAsJsonObject();
            String uic = localInstanceJSON.get("UIC").getAsString();
            List<TrackedEntityInstanceInfo> teiInfos = getInstancesWithUIC(uic, trackedEntityInstanceInfos);
            if (teiInfos.size() == 1) {
                trackedEntityInstanceInfoList.add(teiInfos.get(0));
            } else if (teiInfos.size() > 1) {
                Optional<TrackedEntityInstanceInfo> trackedEntityInstanceInfo = getDeDuplicatedInstance(teiInfos, localInstanceJSON);
                trackedEntityInstanceInfo.ifPresent(trackedEntityInstanceInfoList::add);
            }
        });
        return trackedEntityInstanceInfoList;
    }

    private Optional<TrackedEntityInstanceInfo> getDeDuplicatedInstance(List<TrackedEntityInstanceInfo> teiInfos, JsonObject tableRowJsonObject) {
        List<TrackedEntityInstanceInfo> teiInfoList = teiInfos.stream().filter(teiInfo -> {
            try {
                return isSameClient(teiInfo, tableRowJsonObject);
            } catch (ParseException e) {
                return false;
            }
        }).collect(Collectors.toList());
        if (teiInfoList.size() == 1) {
            return Optional.of(teiInfoList.get(0));
        }
        return Optional.empty();
    }

    private List<TrackedEntityInstanceInfo> getInstancesWithUIC(String uic, List<TrackedEntityInstanceInfo> trackedEntityInstanceInfos) {
        List<TrackedEntityInstanceInfo> teiInfos = new ArrayList<>();
        for (TrackedEntityInstanceInfo trackedEntityInstanceInfo : trackedEntityInstanceInfos) {
            for (Attribute attribute : trackedEntityInstanceInfo.getAttributes()) {
                if (attribute.getAttribute().equals(uicAttributeId)) {
                    if (attribute.getValue().equals(uic)) {
                        teiInfos.add(trackedEntityInstanceInfo);
                    }
                }
            }
        }
        return teiInfos;
    }

    private boolean isSameClient(TrackedEntityInstanceInfo trackedEntityInstanceInfo, JsonObject localInstance) throws ParseException {
        String gender = localInstance.get("Gender").getAsString();
        String mothersFirstName = localInstance.get("Mothers_First_Name").getAsString();
        String dateOfBirth = localInstance.get("Date_of_Birth").getAsString();
        String lastName = localInstance.get("Last_Name").getAsString();
        String districtOfBirth = localInstance.get("District_of_Birth").getAsString();
        String areYouTwin = "false";
        if (localInstance.has("Are_you_Twin")) {
            areYouTwin = localInstance.get("Are_you_Twin").getAsString();
        }
        boolean allComparableAttributesArePresent = trackedEntityInstanceInfo.hasAttribute(mothersFirstNameAttributeId) && trackedEntityInstanceInfo.hasAttribute(genderAttributeId) && trackedEntityInstanceInfo.hasAttribute(lastNameAttributeId) && trackedEntityInstanceInfo.hasAttribute(dateOfBirthAttributeId)
                && trackedEntityInstanceInfo.hasAttribute(districtOfBirthAttributeId) && trackedEntityInstanceInfo.hasAttribute(twinAttributeId);
        if (allComparableAttributesArePresent) {
            boolean mothersNameMatch = trackedEntityInstanceInfo.getAttributeValue(mothersFirstNameAttributeId).equals(mothersFirstName);
            boolean genderMatch = trackedEntityInstanceInfo.getAttributeValue(genderAttributeId).equals(gender);
            boolean lastNameMatch = trackedEntityInstanceInfo.getAttributeValue(lastNameAttributeId).equals(lastName);
            boolean dateOfBirthNameMatch = new SimpleDateFormat("yyyy-MM-dd").parse(dateOfBirth).equals(new SimpleDateFormat("yyyy-MM-dd").parse(trackedEntityInstanceInfo.getAttributeValue(dateOfBirthAttributeId)));
            boolean districtOfBirthMatch = trackedEntityInstanceInfo.getAttributeValue(districtOfBirthAttributeId).equals(districtOfBirth);
            boolean twinMatch = trackedEntityInstanceInfo.getAttributeValue(twinAttributeId).equals(areYouTwin);
            return mothersNameMatch && genderMatch && lastNameMatch && dateOfBirthNameMatch && districtOfBirthMatch && twinMatch;
        }
       return false;
    }

    public Boolean instanceExistsInDHIS(JsonObject tableRowJsonObject, List<TrackedEntityInstanceInfo> trackedEntityInstances) throws ParseException {
        for (TrackedEntityInstanceInfo tei : trackedEntityInstances) {
            if (isSameClient(tei, tableRowJsonObject)) {
                return true;
            }
        }
        return false;
    }

    public void getEnrollmentsForInstances(String enrollmentTable, String eventTable, String programName, String startDate, String endDate) throws Exception {
        logger.info("Enrollment Table is " + enrollmentTable);
        logger.info("Event Table is " + eventTable);
        logger.info("Program name is " + programName);

        TEIUtil.setInstancesWithEnrollments(new HashMap<>());
        List<Map<String, Object>> deltaInstanceIds = (startDate != "" && endDate !="" ) ?
                patientDAO.getDeltaEnrollmentInstanceIdsWithDateRange(enrollmentTable, eventTable, programName, startDate, endDate)
                :patientDAO.getDeltaEnrollmentInstanceIds(enrollmentTable, eventTable, programName);

        logger.info("Delta Instance Ids: " + (deltaInstanceIds != null ? deltaInstanceIds.size() : "null"));

        if (!deltaInstanceIds.isEmpty()) {
            List<String> instanceIdsList = getInstanceIds(deltaInstanceIds);//Get all instance ids for changes
            String program = deltaInstanceIds.get(0).get("program").toString();
            logger.info("instanceIdsList : " + instanceIdsList.size());
            logger.info("program name is ->"+ program);
            int lowerLimit = 0;
            int upperLimit = TEI_FILTER_URI_LIMIT;
            List<TrackedEntityInstanceInfo> result = new ArrayList<>();
            while (lowerLimit < instanceIdsList.size()) {
                if (upperLimit > instanceIdsList.size()) {
                    upperLimit = instanceIdsList.size();
                }
                logger.info("Lower : " + lowerLimit + " Upper " + upperLimit);
                List<String> subInstanceIds = instanceIdsList.subList(lowerLimit, upperLimit);
                lowerLimit = upperLimit;
                upperLimit += TEI_FILTER_URI_LIMIT;

                String instanceIds = String.join(";", subInstanceIds);
                String url = String.format(TEI_ENROLLMENTS_URI, program, instanceIds);

                ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstancesWithEnrollmentInformation = syncRepository.getTrackedEntityInstances(url);
                result.addAll(trackedEntityInstancesWithEnrollmentInformation.getBody().getTrackedEntityInstances());

            }
            TEIUtil.setInstancesWithEnrollments(getMap(result, program));
            logger.info("Results Size " + result.size());
        }
    }

    private Map<String, List<EnrollmentDetails>> getMap(List<TrackedEntityInstanceInfo> trackedEntityInstances, String currentProgram) {
        Map<String, List<EnrollmentDetails>> instancesMap = new HashMap<>();
        trackedEntityInstances.forEach(trackedEntityInstance -> {
            if (!trackedEntityInstance.getEnrollments().isEmpty()) {
                instancesMap.put(trackedEntityInstance.getTrackedEntityInstance(), filterProgramsBy(currentProgram, trackedEntityInstance.getEnrollments()));
            }
        });
        logger.info("instancesMap " + instancesMap);
        return instancesMap;
    }

    private List<EnrollmentDetails> filterProgramsBy(String program, List<EnrollmentDetails> allEnrollments) {
        return allEnrollments.stream().filter(enrollment -> program.equals(enrollment.getProgram())).collect(Collectors.toList());
    }

    private List<String> getInstanceIds(List<Map<String, Object>> newEnrollmentInstances) {
        return newEnrollmentInstances
                .stream()
                .map(instanceObj -> instanceObj.get("instance_id").toString())
                .collect(Collectors.toList());
    }

    private List<List<Map<String, Object>>> separateSearchFieldsBasedOnFilterLimit(List<Map<String, Object>> searchableFields) {
        List<List<Map<String, Object>>> result = new ArrayList<>();
        List<Map<String, Object>> searchableGroup = new ArrayList<>();
        searchableFields.forEach(searchableField -> {
            if (searchableGroup.size() >= TEI_FILTER_URI_LIMIT) {
                result.add(new ArrayList<>(searchableGroup));
                searchableGroup.clear();
            }
            searchableGroup.add(searchableField);
        });

        if (!searchableGroup.isEmpty()) {
            result.add(searchableGroup);
        }

        return result;
    }

    public Map<String, String> verifyOrgUnitsForPatients(LookupTable lookupTable, String serviceName) {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getInstance(), CATEGORY_INSTANCE, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEnrollments(), CATEGORY_NEW_ACTIVE_ENROLLMENT, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEnrollments(), CATEGORY_NEW_COMPLETED_ENROLLMENT, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEnrollments(), CATEGORY_NEW_CANCELLED_ENROLLMENT, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEnrollments(), CATEGORY_UPDATED_ACTIVE_ENROLLMENT, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEnrollments(), CATEGORY_UPDATED_COMPLETED_ENROLLMENT, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEnrollments(), CATEGORY_UPDATED_CANCELLED_ENROLLMENT, serviceName)));
        rows.addAll(jdbcTemplate.queryForList(String.format(RECORDS_WITH_INVALID_ORG_UNIT_QUERY,
                lookupTable.getEvent(), CATEGORY_EVENT, serviceName)));

        Map<String, String> invalidPatients = new HashMap<>();
        rows.forEach(row -> {
            String patientID = (String) row.get("Patient_Identifier");
            String orgUnit = (String) row.get("OrgUnit");
            invalidPatients.put(patientID, orgUnit);
        });
        return invalidPatients;
    }

    public List<TrackedEntityInstanceInfo> getTrackedEntityInstancesForUIC(String uic) throws Exception {
        StringBuilder url = new StringBuilder();
        url.append(TEI_URI).append("&fields=[*]").append("&ou=").append(orgUnitID).append("&ouMode=DESCENDANTS").append("&filter=").append(uicAttributeId).append(":IN:").append(uic).append(";").append("&includeAllAttributes=true");

        ResponseEntity<TrackedEntityInstanceResponse> response = syncRepository.getTrackedEntityInstances(url.toString());
        if (response.getStatusCode().is2xxSuccessful()) {
            return response.getBody().getTrackedEntityInstances();
        }
        return Collections.emptyList();
    }
}