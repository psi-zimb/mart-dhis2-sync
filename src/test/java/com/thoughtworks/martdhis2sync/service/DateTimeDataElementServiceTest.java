package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.DataElement;
import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.model.Pager;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.DataElementsUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.ResponseEntity;

import javax.sql.DataSource;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class DateTimeDataElementServiceTest {
    private DateTimeDataElementService dateTimeDataElementService;

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private DataSource dataSource;

    private ResponseEntity<DataElementResponse> responseEntity;
    private List<DataElement> dataElements = new LinkedList<>();

    @Before
    public void setUp() throws Exception {
        dateTimeDataElementService = new DateTimeDataElementService();
        setValuesForMemberFields(dateTimeDataElementService, "syncRepository", syncRepository);

        dataElements.add(new DataElement("asfasdfs", "date"));
        dataElements.add(new DataElement("gdfdsfsf", "time"));
        mockStatic(DataElementsUtil.class);
    }

    @Test
    public void shouldGetDataElementInfoFromDHIS() {
        responseEntity = ResponseEntity.ok(new DataElementResponse(new Pager(), dataElements));

        when(syncRepository.getDataElements("")).thenReturn(responseEntity);

        dateTimeDataElementService.getDataElements();

        verify(syncRepository, times(1)).getDataElements("");
    }
}
