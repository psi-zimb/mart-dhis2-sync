package com.thoughtworks.martdhis2sync.repository;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.JobService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;


import java.nio.charset.Charset;
import java.util.List;

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
}
