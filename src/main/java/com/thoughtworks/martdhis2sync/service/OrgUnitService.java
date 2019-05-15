package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.OrgUnit;
import com.thoughtworks.martdhis2sync.model.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.OrgUnitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Component
public class OrgUnitService {

    @Value("${dhis2.url}")
    private String dhis2Url;

    @Value("${country.org.unit.id.for.patient.data.duplication.check}")
    private String rootOrgUnit;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private SyncRepository syncRepository;

    private static final String URI_ORG_UNIT = "/api/organisationUnits/";

    private static final String QUERY_PARAMS = "includeDescendants=true";

    private List<OrgUnit> orgUnits = new ArrayList<>();

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "OrgUnit Service: ";

    @Scheduled(cron = "${org.unit.cron.interval}")
    @PostConstruct
    public void getOrgUnitsList() throws SQLException {

        logger.info(LOG_PREFIX + "Started.");
        orgUnits.clear();
        String url = dhis2Url + URI_ORG_UNIT + rootOrgUnit + "?" + QUERY_PARAMS;
        ResponseEntity<OrgUnitResponse> responseEntity = syncRepository.getOrgUnits(url);
        if (null == responseEntity) {
            return;
        }
        orgUnits.addAll(responseEntity.getBody().getOrganisationUnits());

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
                    ps.setString(1, ou.getCode());
                    ps.setString(2, ou.getId());
                    ps.setTimestamp(3, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }
}
