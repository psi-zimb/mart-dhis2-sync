package com.thoughtworks.martdhis2sync.repository;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestTemplate;

@Repository
public class SyncRepository {

    @Value("${dhis2.url}")
    private String dhis2Url;

    public void sendData(String uri, String body) {

        ResponseEntity<String> stringResponseEntity = new RestTemplate().postForEntity(dhis2Url + uri, body, String.class);
        System.out.println(stringResponseEntity);

    }
}
