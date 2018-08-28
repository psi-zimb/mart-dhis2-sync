package com.thoughtworks.martdhis2sync.repository;

import com.thoughtworks.martdhis2sync.response.TrackedEntityResponse;
import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;

@Repository
public class SyncRepository {

    @Value("${dhis2.url}")
    private String dhis2Url;

    public ResponseEntity<TrackedEntityResponse> sendData(String uri, String body) {
        ResponseEntity<TrackedEntityResponse> responseEntity = null;
        try {
            responseEntity = new RestTemplate()
                    .exchange(dhis2Url + uri, HttpMethod.POST, getRequestEntity(body), TrackedEntityResponse.class);

        } catch (Exception e) {
            //logger.error();
        }
        return responseEntity;
    }

    private HttpEntity getRequestEntity(String body) {
        String auth = "admin" + ":" + "district";
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        httpHeaders.set("Authorization", authHeader);

        return new HttpEntity<>(body, httpHeaders);
    }
}
