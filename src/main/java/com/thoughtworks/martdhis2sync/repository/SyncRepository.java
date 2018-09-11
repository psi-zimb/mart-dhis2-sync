package com.thoughtworks.martdhis2sync.repository;

import com.thoughtworks.martdhis2sync.response.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.response.TrackedEntityResponse;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;

@Repository
public class SyncRepository {

    @Value("${dhis2.url}")
    private String dhis2Url;

    @Value("${dhis2.user}")
    private String dhisUser;

    @Value("${dhis2.password}")
    private String dhisPassword;

    @Value("${uri.org.unit}")
    private String orgUnitUri;

    private Logger logger = LoggerFactory.getLogger(SyncRepository.class);

    private static final String logPrefix = "SyncRepository: ";

    public ResponseEntity<TrackedEntityResponse> sendData(String uri, String body) {
        ResponseEntity<TrackedEntityResponse> responseEntity = null;
        try {
            responseEntity = new RestTemplate()
                    .exchange(dhis2Url + uri, HttpMethod.POST, new HttpEntity<>(body, getHttpHeaders()), TrackedEntityResponse.class);
        } catch (Exception e) {
            logger.error(logPrefix + e);
        }
        return responseEntity;
    }

    public ResponseEntity<OrgUnitResponse> getOrgUnits(String url) {
        ResponseEntity<OrgUnitResponse> responseEntity = null;
        try {
            responseEntity = new RestTemplate()
                    .exchange((url.isEmpty() ? dhis2Url + orgUnitUri : url), HttpMethod.GET,
                            new HttpEntity<>(getHttpHeaders()), OrgUnitResponse.class);
        } catch (Exception e) {
            logger.error(logPrefix + e);
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
