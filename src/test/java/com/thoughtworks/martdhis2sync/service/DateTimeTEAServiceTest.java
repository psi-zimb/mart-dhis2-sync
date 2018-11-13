package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.Pager;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttribute;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttributeResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.ResponseEntity;

import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class DateTimeTEAServiceTest {

    private DateTimeTEAService dateTimeTEAService;

    @Mock
    private SyncRepository syncRepository;

    private ResponseEntity<TrackedEntityAttributeResponse> trackedEntityAttributeResponse;

    private List<TrackedEntityAttribute> trackedEntityAttributes = new LinkedList<>();

    @Before
    public void setUp() throws Exception {
        dateTimeTEAService = new DateTimeTEAService();
        setValuesForMemberFields(dateTimeTEAService, "syncRepository", syncRepository);
        trackedEntityAttributes.add(new TrackedEntityAttribute("asfsadf", "Date"));
        trackedEntityAttributes.add(new TrackedEntityAttribute("dfdfgdd", "Time"));
    }

    @Test
    public void shouldGetAllTEAttributesOfTypeDateAndTime(){
        trackedEntityAttributeResponse = ResponseEntity.ok(new TrackedEntityAttributeResponse(new Pager(), trackedEntityAttributes));

        when(syncRepository.getTrackedEntityAttributes("")).thenReturn(trackedEntityAttributeResponse);

        dateTimeTEAService.getTEAttributes();

        verify(syncRepository, times(1)).getTrackedEntityAttributes("");
    }
}
