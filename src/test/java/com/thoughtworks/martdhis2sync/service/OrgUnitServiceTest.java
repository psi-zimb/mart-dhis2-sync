package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.response.OrgUnit;
import com.thoughtworks.martdhis2sync.response.OrgUnitResponse;
import com.thoughtworks.martdhis2sync.response.Pager;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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

    @Before
    public void setUp() throws Exception {
        orgUnitService = new OrgUnitService();
        setValuesForMemberFields(orgUnitService, "syncRepository", syncRepository);
        setValuesForMemberFields(orgUnitService, "dataSource", dataSource);
    }

    @Test
    @SneakyThrows
    public void shouldGetAllOrgUnitsFromDHIS() {
        responseEntity = ResponseEntity.ok(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits("")).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        orgUnitService.getOrgUnitsList();

        verify(syncRepository, times(1)).getOrgUnits("");
        verify(dataSource, times(1)).getConnection();
    }

    @Test(expected = SQLException.class)
    @SneakyThrows
    public void shouldHandleExceptionsThrownWhenTryingToGetOrgUnitsFromDHIS() {
        responseEntity = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new OrgUnitResponse(new Pager(), orgUnitList));

        when(syncRepository.getOrgUnits("")).thenReturn(responseEntity);
        when(dataSource.getConnection()).thenThrow(new SQLException());

        orgUnitService.getOrgUnitsList();

        verify(syncRepository, times(1)).getOrgUnits("");
        verify(dataSource, times(0)).getConnection();
    }

}