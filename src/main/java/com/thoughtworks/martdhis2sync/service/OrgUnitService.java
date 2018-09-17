package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.model.OrgUnit;
import com.thoughtworks.martdhis2sync.model.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.OrgUnitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Component
@EnableScheduling
public class OrgUnitService {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private SyncRepository syncRepository;

    private List<OrgUnit> orgUnits = new ArrayList<>();

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "OrgUnit Service: ";

    @Scheduled(cron = "${org.unit.cron.interval}")
    public void getOrgUnitsList() throws SQLException {

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
        int count = updateTracker();
        logger.info(LOG_PREFIX + "Saved " + count + " Org Units");
    }

    private int updateTracker() throws SQLException {

        String deleteQuery = "DELETE FROM public.orgunit_tracker";
        String insertQuery = "INSERT INTO public.orgunit_tracker(orgunit, id, date_created) values (?, ?, ?)";
        int updateCount = 0;

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(deleteQuery)) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = connection.prepareStatement(insertQuery)) {
                for (OrgUnit ou : orgUnits) {
                    ps.setString(1, ou.getDisplayName());
                    ps.setString(2, ou.getId());
                    ps.setTimestamp(3, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }
}
