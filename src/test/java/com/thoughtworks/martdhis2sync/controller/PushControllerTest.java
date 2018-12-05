package com.thoughtworks.martdhis2sync.controller;

import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.service.CompletedEnrollmentService;
import com.thoughtworks.martdhis2sync.service.DHISMetaDataService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.service.MappingService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.SyncFailedException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;

@RunWith(PowerMockRunner.class)
public class PushControllerTest {
    @Mock
    private MappingService mappingService;

    @Mock
    private TEIService teiService;

    @Mock
    private LoggerService loggerService;

    @Mock
    private DHISMetaDataService dhisMetaDataService;

    @Mock
    private CompletedEnrollmentService completedEnrollmentService;

    @Mock
    private MarkerUtil markerUtil;

    private PushController pushController;
    private String service = "HT Service";
    private String user = "admin";
    private String comment = "";
    Date lastSyncedDate = new Date(Long.MIN_VALUE);

    @Before
    public void setUp() throws Exception {
        pushController = new PushController();
        setValuesForMemberFields(pushController, "mappingService", mappingService);
        setValuesForMemberFields(pushController, "teiService", teiService);
        setValuesForMemberFields(pushController, "loggerService", loggerService);
        setValuesForMemberFields(pushController, "dhisMetaDataService", dhisMetaDataService);
        setValuesForMemberFields(pushController, "completedEnrollmentService", completedEnrollmentService);
        setValuesForMemberFields(pushController, "markerUtil", markerUtil);


        when(markerUtil.getLastSyncedDate(service, "enrollment")).thenReturn(lastSyncedDate);
        when(markerUtil.getLastSyncedDate(service, "event")).thenReturn(lastSyncedDate);
    }

    @Test
    public void shouldNotCallEnrollmentServiceWhenTeiServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(dhisMetaDataService).getTrackedEntityInstances(getDhisSyncRequestBody().getService());
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doThrow(new SyncFailedException("instance sync failed")).when(teiService).triggerJob(anyString(), anyString(), anyString(), any());

        pushController.pushData(dhisSyncRequestBody);

        verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
        verify(dhisMetaDataService, times(1)).getTrackedEntityInstances(
                getDhisSyncRequestBody().getService()
        );
        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
        verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
        verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
    }

    @Test
    public void shouldNotCallUpdatedCompletedEnrollmentServiceWhenNewCompletedEnrollmentIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any());
        doThrow(new SyncFailedException("instance sync failed")).when(completedEnrollmentService)
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any());

        pushController.pushData(dhisSyncRequestBody);

        verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
        verify(completedEnrollmentService, times(1))
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any());
        verify(completedEnrollmentService, times(0)).triggerJobForUpdatedCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any());
        verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
        verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
    }

    @Test
    public void shouldThrowExceptionWithNoDataToSync() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).getTrackedEntityInstances(getDhisSyncRequestBody().getService());
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "success");
        doNothing().when(loggerService).collateLogMessage("No delta data to sync.");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any());
        doNothing().when(completedEnrollmentService).triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(),anyString(),  any());
        doNothing().when(completedEnrollmentService).triggerJobForUpdatedCompletedEnrollments(anyString(), anyString(), anyString(),anyString(),  any());

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (Exception e ) {
            verify(loggerService, times(1)).addLog(service, user, comment);
            verify(dhisMetaDataService, times(1)).getTrackedEntityInstances(
                    getDhisSyncRequestBody().getService()
            );
            verify(loggerService, times(1)).updateLog(service, "success");
            verify(loggerService, times(1)).collateLogMessage("No delta data to sync.");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
            verify(completedEnrollmentService, times(1)).triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any());
            verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");

            assertEquals("NO DATA TO SYNC", e.getMessage());
        }
    }

    private DHISSyncRequestBody getDhisSyncRequestBody() {
        DHISSyncRequestBody dhisSyncRequestBody = new DHISSyncRequestBody();
        dhisSyncRequestBody.setService(service);
        dhisSyncRequestBody.setUser(user);
        dhisSyncRequestBody.setComment(comment);
        return dhisSyncRequestBody;
    }

    private Map<String, Object> getMapping() {
        String lookupTable = "{" +
                "\"instance\":\"hts_instance_table\"," +
                "\"enrollments\":\"hts_program_enrollment_table\"," +
                "\"event\":\"hts_program_events_table\"" +
            "}";

        String mappingJson = "{" +
                "\"instance\":" +
                    "{" +
                        "\"Patient_Identifier\":\"\"," +
                        "\"UIC\":\"rOb34aQLSyC\"" +
                    "}," +
                "\"event\":" +
                    "{" +
                        "\"self_testing_outcome\":\"gwatO1kb3Fy\"," +
                        "\"client_received\":\"gXNu7zJBTDN\"" +
                    "}" +
            "}";

        String config = "{" +
                "\"searchable\":[" +
                "\"UIC\"" +
                "]" +
                "}";

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("mapping_name", service);
        mapping.put("lookup_table", lookupTable);
        mapping.put("mapping_json", mappingJson);
        mapping.put("config", config);
        return mapping;
    }
}
