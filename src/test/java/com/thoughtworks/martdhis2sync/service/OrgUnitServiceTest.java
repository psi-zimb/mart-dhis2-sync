package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.model.OrgUnit;
import com.thoughtworks.martdhis2sync.model.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.model.Pager;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class OrgUnitServiceTest {

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    private List<OrgUnit> orgUnitList = Arrays.asList(
            new OrgUnit("OU1", "abcdef"),
            new OrgUnit("OU2", "ghijkl"));

    private OrgUnitService orgUnitService;

    private ResponseEntity<OrgUnitResponse> responseEntity;
    private String dhis2Url = "http://play.dhis2.org";
    private String URI_ORG_UNIT = "/api/organisationUnits/";
    private String ROOT_ORG_UNIT = "SxgCPPeiq3c";
    private String QUERY_PARAMS = "?fields=:all&pageSize=150000&includeDescendants=true";
    private String URLToGetOrgUnits = dhis2Url + URI_ORG_UNIT + ROOT_ORG_UNIT + QUERY_PARAMS;

    @Before
    public void setUp() throws Exception {
        orgUnitService = new OrgUnitService();
        setValuesForMemberFields(orgUnitService, "syncRepository", syncRepository);
        setValuesForMemberFields(orgUnitService, "dataSource", dataSource);
        setValuesForMemberFields(orgUnitService, "dhis2Url", dhis2Url);
        setValuesForMemberFields(orgUnitService, "rootOrgUnit", ROOT_ORG_UNIT);
    }

    @Test
    @SneakyThrows
    public void shouldGetAllOrgUnitsFromDHIS() {
        responseEntity = ResponseEntity.ok(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        orgUnitService.getOrgUnitsList();

        verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
        verify(dataSource, times(1)).getConnection();
    }

    @Test
    public void shouldHandleExceptionsThrownWhenTryingToGetOrgUnitsFromDHIS() throws SQLException {
        responseEntity = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenThrow(new SQLException());

        try {
            orgUnitService.getOrgUnitsList();
        } catch (SQLException e) {
            verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
            verify(dataSource, times(1)).getConnection();
        }
    }

    @Test
    public void shouldNotUpdateTrackerWhenDhisReturnsNull() throws SQLException {
        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(null);

        orgUnitService.getOrgUnitsList();

        verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
        verify(dataSource, times(0)).getConnection();
    }

    @Test
    public void shouldThrowExceptionWhenPrepareStatementWithDeleteQueryIsFailed() throws SQLException {
        String deleteQuery = "DELETE FROM public.orgunit_tracker";

        responseEntity = ResponseEntity.ok(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(deleteQuery)).thenThrow(new SQLException());

        try {
            orgUnitService.getOrgUnitsList();
        } catch (SQLException e) {
            verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
            verify(dataSource, times(1)).getConnection();
            verify(connection, times(1)).prepareStatement(deleteQuery);
        }
    }

    @Test
    public void shouldThrowExceptionWhenExecuteUpdateWithDeleteQueryIsFailed() throws SQLException {
        String deleteQuery = "DELETE FROM public.orgunit_tracker";

        responseEntity = ResponseEntity.ok(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(deleteQuery)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException());

        try {
            orgUnitService.getOrgUnitsList();
        } catch (SQLException e) {
            verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
            verify(dataSource, times(1)).getConnection();
            verify(connection, times(1)).prepareStatement(deleteQuery);
            verify(preparedStatement, times(1)).executeUpdate();
        }
    }

    @Test
    public void shouldThrowExceptionWhenPrepareStatementWithSelectQueryIsFailed() throws SQLException {
        String deleteQuery = "DELETE FROM public.orgunit_tracker";
        String selectQuery = "INSERT INTO public.orgunit_tracker(orgunit, id, date_created) values (?, ?, ?)";

        responseEntity = ResponseEntity.ok(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(deleteQuery)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);
        when(connection.prepareStatement(selectQuery)).thenThrow(new SQLException());

        try {
            orgUnitService.getOrgUnitsList();
        } catch (SQLException e) {
            verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
            verify(dataSource, times(1)).getConnection();
            verify(connection, times(1)).prepareStatement(deleteQuery);
            verify(connection, times(1)).prepareStatement(selectQuery);
            verify(preparedStatement, times(1)).executeUpdate();
        }
    }

    @Test
    public void shouldThrowExceptionWhenExecuteUpdateWithSelectQueryIsFailed() throws SQLException {
        String deleteQuery = "DELETE FROM public.orgunit_tracker";
        String selectQuery = "INSERT INTO public.orgunit_tracker(orgunit, id, date_created) values (?, ?, ?)";

        responseEntity = ResponseEntity.ok(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits(URLToGetOrgUnits)).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(deleteQuery)).thenReturn(preparedStatement);
        when(connection.prepareStatement(selectQuery)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate())
                .thenReturn(1)
                .thenThrow(new SQLException());

        try {
            orgUnitService.getOrgUnitsList();
        } catch (SQLException e) {
            verify(syncRepository, times(1)).getOrgUnits(URLToGetOrgUnits);
            verify(dataSource, times(1)).getConnection();
            verify(connection, times(1)).prepareStatement(deleteQuery);
            verify(connection, times(1)).prepareStatement(selectQuery);
            verify(preparedStatement, times(2)).executeUpdate();
        }
    }
}
