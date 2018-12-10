package com.thoughtworks.martdhis2sync.controller;

import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.service.*;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.SyncFailedException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TrackersHandler.class)
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
    private ActiveEnrollmentService activeEnrollmentService;

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
        setValuesForMemberFields(pushController, "activeEnrollmentService", activeEnrollmentService);
        setValuesForMemberFields(pushController, "markerUtil", markerUtil);

        mockStatic(TrackersHandler.class);
        doNothing().when(TrackersHandler.class);
        TrackersHandler.clearTrackerLists();
        when(markerUtil.getLastSyncedDate(service, "enrollment")).thenReturn(lastSyncedDate);
        when(markerUtil.getLastSyncedDate(service, "event")).thenReturn(lastSyncedDate);
    }

    @After
    public void tearDown() throws Exception {
        TrackersHandler.clearTrackerLists();
    }

    @Test
    public void shouldNotCallEnrollmentServiceWhenTeiServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(dhisMetaDataService).getTrackedEntityInstances(dhisSyncRequestBody.getService());
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doThrow(new SyncFailedException("instance sync failed")).when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList());

        pushController.pushData(dhisSyncRequestBody);

        verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
        verify(dhisMetaDataService, times(1)).getTrackedEntityInstances(
                getDhisSyncRequestBody().getService()
        );
        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
        verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
        verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
        verifyStatic(times(0));
    }

    @Test
    public void shouldNotCallUpdatedCompletedEnrollmentServiceWhenNewCompletedEnrollmentIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
        doThrow(new SyncFailedException("instance sync failed")).when(completedEnrollmentService)
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        doNothing().when(teiService).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service);

        pushController.pushData(dhisSyncRequestBody);

        verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
        verify(completedEnrollmentService, times(1))
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        verify(completedEnrollmentService, times(0)).triggerJobForUpdatedCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), any(), anyString());
        verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
        verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
        verifyStatic(times(1));
        TrackersHandler.clearTrackerLists();
        verify(teiService, times(1)).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service);
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
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
        doNothing().when(teiService).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service);
        doNothing().when(completedEnrollmentService).triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(),anyString(),  any(), anyString());
        doNothing().when(completedEnrollmentService).triggerJobForUpdatedCompletedEnrollments(anyString(), anyString(), anyString(),anyString(),  any(), any(), anyString());

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (Exception e) {
            verify(loggerService, times(1)).addLog(service, user, comment);
            verify(dhisMetaDataService, times(1)).getTrackedEntityInstances(
                    getDhisSyncRequestBody().getService()
            );
            verify(loggerService, times(1)).updateLog(service, "success");
            verify(loggerService, times(1)).collateLogMessage("No delta data to sync.");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
            verify(completedEnrollmentService, times(1)).triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
            verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(4));
            TrackersHandler.clearTrackerLists();
            verify(teiService, times(1)).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service);

            assertEquals("NO DATA TO SYNC", e.getMessage());
        }
    }

    @Test
    public void shouldNotCallActiveEnrollmentServiceWhenCompletedEnrollmentServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
        doNothing().when(completedEnrollmentService)
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        doNothing().when(completedEnrollmentService)
                .triggerJobForUpdatedCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), any(), anyString());
        doThrow(new SyncFailedException("instance sync failed")).when(completedEnrollmentService)
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());

        pushController.pushData(dhisSyncRequestBody);

        verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(completedEnrollmentService, times(1))
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        verify(activeEnrollmentService, times(0))
                .triggerJobForNewActiveEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
        verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
        verifyStatic(times(1));
    }

    @Test
    public void shouldNotInvokeSecondJobOfActiveEnrollmentServiceIfFirstJobFails() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList());
        doNothing().when(completedEnrollmentService)
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        doNothing().when(completedEnrollmentService)
                .triggerJobForUpdatedCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), any(), any(), anyString());
        doThrow(new SyncFailedException("instance sync failed")).when(activeEnrollmentService)
                .triggerJobForNewActiveEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());

        pushController.pushData(dhisSyncRequestBody);

        verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(activeEnrollmentService, times(1))
                .triggerJobForNewActiveEnrollments(anyString(), anyString(), anyString(), anyString(), any(), anyString());
        verify(activeEnrollmentService, times(0))
                .triggerJobForUpdatedActiveEnrollments(anyString(), anyString(), anyString(), anyString(), any(), any(), anyString());
        verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
        verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
        verifyStatic(times(3));
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
                "]," +
                "\"openLatestCompletedEnrollment\": \"no\"" +
                "}";

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("mapping_name", service);
        mapping.put("lookup_table", lookupTable);
        mapping.put("mapping_json", mappingJson);
        mapping.put("config", config);
        return mapping;
    }
}
