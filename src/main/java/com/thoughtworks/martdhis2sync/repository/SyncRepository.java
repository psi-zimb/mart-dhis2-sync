package com.thoughtworks.martdhis2sync.repository;

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

    public void sendData(String uri, String body) {
        System.out.println("url" + dhis2Url+uri);
        System.out.println("body" + body);

        ResponseEntity<String> stringResponseEntity = null;
            stringResponseEntity = new RestTemplate()
                    .exchange(dhis2Url + uri, HttpMethod.POST, getRequestEntity(body), String.class);
        System.out.println(stringResponseEntity);
    }

    private HttpEntity getRequestEntity(String body) {
        String auth = "admin" + ":" + "district";
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")) );
        String authHeader = "Basic " + new String( encodedAuth );

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        httpHeaders.set("Authorization", authHeader );

        return new HttpEntity<>(body, httpHeaders);
    }
}
