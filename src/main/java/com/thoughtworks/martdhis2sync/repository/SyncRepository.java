package com.thoughtworks.martdhis2sync.repository;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.model.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttributeResponse;
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

import static com.thoughtworks.martdhis2sync.service.DHISMetaDataService.URI_DATE_TIME_DATA_ELEMENTS;
import static com.thoughtworks.martdhis2sync.service.DHISMetaDataService.URI_DATE_TIME_T_E_ATTRIBUTES;
import static com.thoughtworks.martdhis2sync.service.OrgUnitService.URI_ORG_UNIT;

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

    public ResponseEntity<DHISSyncResponse> sendData(String uri, String body) {
        ResponseEntity<DHISSyncResponse> responseEntity;
        System.out.println("\nREQ: " + body);
        try {
            responseEntity = restTemplate
                    .exchange(dhis2Url + uri, HttpMethod.POST, new HttpEntity<>(body, getHttpHeaders()), DHISSyncResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
        } catch (HttpClientErrorException e) {
            responseEntity = new ResponseEntity<>(
                    new Gson().fromJson(e.getResponseBodyAsString(), DHISSyncResponse.class),
                    e.getStatusCode());
            loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
            logger.error(LOG_PREFIX + e);
        } catch (HttpServerErrorException e) {
            loggerService.collateLogMessage(String.format("%s %s", e.getStatusCode(), e.getStatusText()));
            logger.error(LOG_PREFIX + e);
            throw e;
        }
        System.out.println("\nRES: " + responseEntity);
        return responseEntity;
    }

    public ResponseEntity<OrgUnitResponse> getOrgUnits(String url) {
        ResponseEntity<OrgUnitResponse> responseEntity = null;
        try {
            responseEntity = restTemplate
                    .exchange((url.isEmpty() ? dhis2Url + URI_ORG_UNIT : url), HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), OrgUnitResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e);
        }
        return responseEntity;
    }

    public ResponseEntity<DataElementResponse> getDataElements(String url) {
        ResponseEntity<DataElementResponse> responseEntity = null;
        try {
            responseEntity = restTemplate
                    .exchange((url.isEmpty() ? dhis2Url + URI_DATE_TIME_DATA_ELEMENTS : url), HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), DataElementResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");

        }catch (Exception e){
            logger.error(LOG_PREFIX + e);
        }
        return responseEntity;
    }

    public ResponseEntity<TrackedEntityAttributeResponse> getTrackedEntityAttributes(String url) {
        ResponseEntity<TrackedEntityAttributeResponse> responseEntity = null;
        try {
            responseEntity = restTemplate
                    .exchange((url.isEmpty() ? dhis2Url + URI_DATE_TIME_T_E_ATTRIBUTES : url), HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), TrackedEntityAttributeResponse.class);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");

        }catch (Exception e){
            logger.error(LOG_PREFIX + e);
        }
        return responseEntity;
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
}
