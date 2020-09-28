package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.Attribute;
import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceInfo;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.responseHandler.EventResponseHandler;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(TEIUtil.class)
public class EnrollmentServiceTest {
    @Mock
    protected SyncRepository syncRepository;
    @Mock
    protected EnrollmentResponseHandler enrollmentResponseHandler;
    @Mock
    protected EventResponseHandler eventResponseHandler;
    private EnrollmentService service;

    @Before
    public void setUp() throws Exception {
        service = new EnrollmentService();

        setValuesForMemberFields(service, "syncRepository", syncRepository);
        setValuesForMemberFields(service, "preferredProgramToAutoEnroll", "zRA");
        setValuesForMemberFields(service, "enrollmentResponseHandler", enrollmentResponseHandler);
        setValuesForMemberFields(service, "eventResponseHandler", eventResponseHandler);
        mockStatic(TEIUtil.class);
        doNothing().when(TEIUtil.class);
        TEIUtil.setTrackedEntityInstanceInfos(getTrackedEntityInstances());
    }

    @Test
    public void enrollSingleClientInstanceToPreferredProgram() throws Exception {
        ResponseEntity<DHISEnrollmentSyncResponse> enrollmentResponse = Mockito.mock(ResponseEntity.class);
        ResponseEntity<TrackedEntityInstanceResponse> response = Mockito.mock(ResponseEntity.class);
        TrackedEntityInstanceResponse teiResponse = Mockito.mock(TrackedEntityInstanceResponse.class);

        when(enrollmentResponse.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getBody()).thenReturn(teiResponse);
        when(response.getBody().getTrackedEntityInstances()).thenReturn(getTrackedEntityInstances());
        when(syncRepository.getTrackedEntityInstances(anyString())).thenReturn(response);
        when(syncRepository.sendEnrollmentData(anyString(), anyString())).thenReturn(enrollmentResponse);
        when(TEIUtil.getTrackedEntityInstanceInfos()).thenReturn(getTrackedEntityInstances());

        service.enrollSingleClientInstanceToPreferredProgram(getTrackedEntityInstances().get(0));

        verify(syncRepository, times(1)).getTrackedEntityInstances(anyString());
    }

    private List<TrackedEntityInstanceInfo> getTrackedEntityInstances() {
        List<TrackedEntityInstanceInfo> trackedEntityInstanceInfos = new LinkedList<>();
        List<Attribute> attributesOfPatient1 = new ArrayList<>();
        List<Attribute> attributesOfPatient2 = new ArrayList<>();

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "***REMOVED***",
                "MMD_PER_NAM",
                "First name",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "w75KJ2mc4zz",
                "Michel"
        ));

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "***REMOVED***",
                "",
                "Last name",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "zDhUuAYrxNC",
                "Jackson"
        ));

        trackedEntityInstanceInfos.add(new TrackedEntityInstanceInfo(
                "2018-09-21T17:54:00.294",
                "SxgCPPeiq3c",
                "2018-09-21T17:54:01.337",
                "w3MoRtzP4SO",
                "2018-09-21T17:54:01.337",
                "o0kaqrZa79Y",
                "2018-09-21T17:54:01.337",
                false,
                false,
                "NONE",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                attributesOfPatient1
        ));

        attributesOfPatient2.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "***REMOVED***",
                "MMD_PER_NAM",
                "First name",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "w75KJ2mc4zz",
                "Jinny"
        ));

        attributesOfPatient2.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "***REMOVED***",
                "",
                "Last name",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "zDhUuAYrxNC",
                "Jackson"
        ));

        trackedEntityInstanceInfos.add(new TrackedEntityInstanceInfo(
                "2018-09-22T13:24:00.24",
                "SxgCPPeiq3c",
                "2018-09-21T17:54:01.337",
                "tzP4SOw3MoR",
                "2018-09-22T13:24:00.241",
                "o0kaqrZa79Y",
                "2018-09-21T17:54:01.337",
                false,
                false,
                "NONE",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                attributesOfPatient2
        ));

        return trackedEntityInstanceInfos;
    }
}