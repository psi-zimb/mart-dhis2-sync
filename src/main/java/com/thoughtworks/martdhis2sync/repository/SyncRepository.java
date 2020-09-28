package com.thoughtworks.martdhis2sync.repository;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class SyncRepository {

    @Value("${dhis2.url}")
    private String dhis2Url;

    @Value("${dhis2.user}")
    private String dhisUser;

    @Value("${dhis2.password}")
    private String dhisPassword;

    @Autowired
    private LoggerService loggerService;

    @Autowired
    private RestTemplate restTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "SyncRepository: ";

    public ResponseEntity<DHISSyncResponse> sendData(String uri, String body) throws Exception {
        return sync(uri, body, DHISSyncResponse.class);
    }

    public ResponseEntity<DHISEnrollmentSyncResponse> sendEnrollmentData(String uri, String body) throws Exception {
        return sync(uri, body, DHISEnrollmentSyncResponse.class);
    }

    public ResponseEntity<DHISEnrollmentSyncResponse> sendEnrollmentDataForUpdate(String uri, String body) throws Exception {
        return syncEnrollments(uri, body, DHISEnrollmentSyncResponse.class);
    }

    public ResponseEntity<OrgUnitResponse> getOrgUnits(String url) {
        ResponseEntity<OrgUnitResponse> responseEntity = null;
        try {
            responseEntity = restTemplate
                    .exchange(url, HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), OrgUnitResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e);
        }
        return responseEntity;
    }

    public ResponseEntity<DataElementResponse> getDataElements(String url) throws Exception {
        ResponseEntity<DataElementResponse> responseEntity = null;
        try {
            responseEntity = restTemplate
                    .exchange(url, HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), DataElementResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");

        }catch (Exception e){
            logger.error(LOG_PREFIX + e);
            throw e;
        }
        return responseEntity;
    }

    public ResponseEntity<TrackedEntityAttributeResponse> getTrackedEntityAttributes(String url) throws Exception {
        ResponseEntity<TrackedEntityAttributeResponse> responseEntity = null;
        try {
            responseEntity = restTemplate
                    .exchange(url, HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), TrackedEntityAttributeResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");

        }catch (Exception e){
            logger.error(LOG_PREFIX + e);
            throw e;
        }
        return responseEntity;
    }

    public ResponseEntity<TrackedEntityInstanceResponse> getTrackedEntityInstances(String uri) throws Exception {
        ResponseEntity<TrackedEntityInstanceResponse> responseEntity = null;
        try {
            logger.info("Tracked Entity Request URI---> "+ uri);

            responseEntity = restTemplate
                    .exchange(dhis2Url + uri, HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), TrackedEntityInstanceResponse.class);

            logger.info("Response---------->\n" + responseEntity);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
            return responseEntity;
        } catch (HttpClientErrorException e) {
            responseEntity = new ResponseEntity<>(
                    new Gson().fromJson(e.getResponseBodyAsString(), TrackedEntityInstanceResponse.class),
                    e.getStatusCode());
            TrackedEntityInstanceResponse body = responseEntity.getBody();
            logger.error("HttpClientErrorException -> " + responseEntity.getBody());
            loggerService.collateLogMessage(String.format("%s %s", body.getHttpStatusCode(), body.getMessage()));
            logger.error(LOG_PREFIX + e);
            throw e;
        } catch (HttpServerErrorException e) {
            loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
            logger.error(LOG_PREFIX + e);
            throw e;
        } catch (Exception e) {
            loggerService.collateLogMessage(String.format("Exception message : %s %nCaused by : %s", e.getMessage(), e.getCause()));
            throw e;
        }
    }

    private HttpHeaders getHttpHeaders() {
        String auth = dhisUser + ":" + dhisPassword;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        httpHeaders.set("Authorization", authHeader);

        return httpHeaders;
    }

    private <T> ResponseEntity<T> sync(String uri, String body, Class<T> type) throws Exception {
        ResponseEntity<T> responseEntity;
        try {

            logger.info("Request URI---> "+ uri);
            logger.info("Request body--->\n"+ body);
            responseEntity = restTemplate
                    .exchange(dhis2Url + uri, HttpMethod.POST, new HttpEntity<>(body, getHttpHeaders()), type);

            logger.info("Response---------->\n" + responseEntity);

            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
        } catch (HttpClientErrorException e) {
            responseEntity = new ResponseEntity<>(
                    new Gson().fromJson(e.getResponseBodyAsString(), type),
                    e.getStatusCode());
            logger.error("e.getResponseBodyAsString() -> " + e.getResponseBodyAsString());
            if (e.getStatusCode().value() == 409) {
                if (e.getResponseBodyAsString().contains("conflicts")) {
                    DHISSyncResponse response = (DHISSyncResponse)responseEntity.getBody();
                    if(response.getResponse() != null)
                    {
                        if(response.getResponse().getImportSummaries() != null)
                        {
                            List<Conflict> conflicts = response.getResponse().getImportSummaries().get(0).getConflicts();
                            for(Conflict conflict : conflicts)
                                loggerService.collateLogMessage(conflict.getValue());
                        }
                    }
                } else {
                    loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
                }
            }
            logger.error("HttpClientErrorException -> " + responseEntity.getBody());
            logger.error(LOG_PREFIX + e);
        } catch (HttpServerErrorException e) {
            if (e.getStatusCode().value() == 502) {
                loggerService.collateLogMessage("DHIS System is Having Issues to Connect. Please try again");
            } else {
                loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
            }
            logger.error(LOG_PREFIX + e);
            throw e;
        } catch (Exception e) {
            loggerService.collateLogMessage(String.format("Exception message : %s %nCaused by : %s", e.getMessage(), e.getCause()));
            throw e;
        }
        return responseEntity;
    }

    private <T> ResponseEntity<T> syncEnrollments(String uri, String body, Class<T> type) throws Exception {
        ResponseEntity<T> responseEntity = null;
        try {

            logger.info("Request URI---> "+ uri);
            logger.info("Request body--->\n"+ body);
            logger.info("ISDATARANGESYNC ->"+ PushController.IS_DATE_RANGE_SYNC);
            boolean dhisSync = PushController.IS_DATE_RANGE_SYNC ? compareEvents(body) : false;
            logger.info("Should Send to DHIS or Not ? ------>" + dhisSync);
            if(dhisSync){
                responseEntity = restTemplate
                        .exchange(dhis2Url + uri, HttpMethod.POST, new HttpEntity<>(body, getHttpHeaders()), type);

                logger.info("Response---------->\n" + responseEntity);

                logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
                return responseEntity;
            }
        } catch (HttpClientErrorException e) {
            responseEntity = new ResponseEntity<>(
                    new Gson().fromJson(e.getResponseBodyAsString(), type),
                    e.getStatusCode());
            logger.error("e.getResponseBodyAsString() -> " + e.getResponseBodyAsString());
            if (e.getStatusCode().value() == 409) {
                if (e.getResponseBodyAsString().contains("conflicts")) {
                    DHISSyncResponse response = (DHISSyncResponse)responseEntity.getBody();
                    if(response.getResponse() != null)
                    {
                        if(response.getResponse().getImportSummaries() != null)
                        {
                            List<Conflict> conflicts = response.getResponse().getImportSummaries().get(0).getConflicts();
                            for(Conflict conflict : conflicts)
                                loggerService.collateLogMessage(conflict.getValue());
                        }
                    }
                } else {
                    loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
                }
            }
            logger.error("HttpClientErrorException -> " + responseEntity.getBody());
            logger.error(LOG_PREFIX + e);
        } catch (HttpServerErrorException e) {
            if (e.getStatusCode().value() == 502) {
                loggerService.collateLogMessage("DHIS System is Having Issues to Connect. Please try again");
            } else {
                loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
            }
            logger.error(LOG_PREFIX + e);
            throw e;
        } catch (Exception e) {
            loggerService.collateLogMessage(String.format("Exception message : %s %nCaused by : %s", e.getMessage(), e.getCause()));
            throw e;
        }
        return responseEntity;
    }

    private boolean compareEvents(String requestBody) {
        Gson jsonFormatter = new Gson();
        EnrollmentsList list = jsonFormatter.fromJson(requestBody,EnrollmentsList.class);
        EnrollmentAPIPayLoadTemp enrollmentData = list.getEnrollments() != null ? list.getEnrollments().get(0) : null;
        logger.info("EnrollmentAPIPayload ->" + enrollmentData.toString());
        if(enrollmentData != null && enrollmentData.getEvents()!=null && enrollmentData.getEvents().size() != 0 ) {
            List<EnrollmentDetails> enrollmentDetails = TEIUtil.getInstancesWithEnrollments().get(enrollmentData.getEvents().get(0).getTrackedEntityInstance());
            logger.info("Enrollment details -> "+ enrollmentDetails);
            Optional<EnrollmentDetails> matchingEnrollmentObject = enrollmentDetails.stream().
                    filter(p -> p.getEnrollment().equals(enrollmentData.getEnrollment())).
                    findFirst();
            EnrollmentDetails enrollment = matchingEnrollmentObject.orElse(null);
            logger.info("Enrollment -> " + enrollment);
            if (enrollment != null) {
                List<EventTemp> source = enrollment.getEvents();
                logger.info("source -> "+ source);
                logger.info("enrollment data events ->"+ enrollmentData.getEvents());
                logger.info("size ->" + enrollmentData.getEvents().size());
                if (source.size() != 0 && enrollmentData.getEvents() != null & enrollmentData.getEvents().size() != 0) {
                    EventTemp latestEvent = source.get(source.size() - 1);
                    //As We are getting events sorted by date updated, we will take latest Event and compare with current Event from Analytics DB
                    EventTemp destination = enrollmentData.getEvents().get(0);
                    logger.info("Source DataValues are ->" + latestEvent.getDataValues());
                    logger.info("Destination DataValues are ->" + destination.getDataValues());
                    if (latestEvent.getDataValues().size() == destination.getDataValues().size()) {
                        logger.info("Source DataValues are ->" + latestEvent.getDataValues());
                        logger.info("Destination DataValues are ->" + destination.getDataValues());
                        if (compareDataValues(latestEvent.getDataValues(), destination.getDataValues()))
                            return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean compareDataValues(List<Map<String, String>> source, List<Map<String, String>> destination) {
        int toMatchCount = destination.size();
        int count = 0;
        for (Map<String, String> temp : destination) {
            if (compareDataValueObjectWRTList(temp, source))
                count++;
        }
        logger.info("Matched count is ->" + count + "\n To be matchedCount is  ->" + toMatchCount);
        if (count == toMatchCount)
            return true;
        else
            return false;
    }

    private boolean compareDataValueObjectWRTList(Map<String, String> temp, List<Map<String, String>> source) {
        for (Map<String, String> temp1 : source) {
            if (temp.entrySet().stream()
                    .allMatch(e -> e.getValue().equals(temp1.get(e.getKey()))))
                return true;
        }
        return false;
    }
}
