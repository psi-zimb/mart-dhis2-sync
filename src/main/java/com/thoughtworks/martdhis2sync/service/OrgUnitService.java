package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.response.OrgUnit;
import com.thoughtworks.martdhis2sync.response.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.util.OrgUnitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@EnableScheduling
public class OrgUnitService {

    @Autowired
    private SyncRepository syncRepository;

    private List<OrgUnit> orgUnits = new ArrayList<>();

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "OrgUnit Service: ";

    @Scheduled(cron = "${org.unit.cron.interval}")
    public void getOrgUnitsList() {

        logger.info(LOG_PREFIX + "Started.");
        orgUnits.clear();
        String uri = "";
        ResponseEntity<OrgUnitResponse> responseEntity;
        do {
            responseEntity = syncRepository.getOrgUnits(uri);
            logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
            if (null == responseEntity) {
                logger.error(LOG_PREFIX + "Received empty response.");
                return;
            }
            orgUnits.addAll(responseEntity.getBody().getOrganisationUnits());
            uri = responseEntity.getBody().getPager().getNextPage();
        } while (null != uri);

        OrgUnitUtil.getOrgUnitMap().clear();
        for (OrgUnit ou : orgUnits) {
            OrgUnitUtil.getOrgUnitMap().put(ou.getDisplayName(), ou.getId());
        }
        logger.info(LOG_PREFIX + "Successfully received and cached "+ OrgUnitUtil.getOrgUnitMap().size() +" Org Units");
    }
}
