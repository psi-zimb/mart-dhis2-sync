package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.model.MappingJson;
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
import org.springframework.web.client.HttpServerErrorException;

import java.io.SyncFailedException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TrackersHandler.class)
public class PushControllerTest {
    Date lastSyncedDate = new Date(Long.MIN_VALUE);
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
    private CancelledEnrollmentService cancelledEnrollmentService;
    @Mock
    private ActiveEnrollmentService activeEnrollmentService;
    @Mock
    private MarkerUtil markerUtil;
    private PushController pushController;
    private String service = "HT Service";
    private String user = "***REMOVED***";
    private String comment = "";
    private Date startDate;
    private Date endDate;

    @Before
    public void setUp() throws Exception {
        pushController = new PushController();
        setValuesForMemberFields(pushController, "mappingService", mappingService);
        setValuesForMemberFields(pushController, "teiService", teiService);
        setValuesForMemberFields(pushController, "loggerService", loggerService);
        setValuesForMemberFields(pushController, "dhisMetaDataService", dhisMetaDataService);
        setValuesForMemberFields(pushController, "completedEnrollmentService", completedEnrollmentService);
        setValuesForMemberFields(pushController, "cancelledEnrollmentService", cancelledEnrollmentService);
        setValuesForMemberFields(pushController, "activeEnrollmentService", activeEnrollmentService);
        setValuesForMemberFields(pushController, "markerUtil", markerUtil);

        mockStatic(TrackersHandler.class);
        doNothing().when(TrackersHandler.class);
        TrackersHandler.clearTrackerLists();
        when(markerUtil.getLastSyncedDate(service, "enrollment")).thenReturn(lastSyncedDate);
        when(markerUtil.getLastSyncedDate(service, "event")).thenReturn(lastSyncedDate);
        startDate = new Date();
        endDate = startDate;
    }

    @After
    public void tearDown() throws Exception {
        TrackersHandler.clearTrackerLists();
    }

    @Test
    public void shouldNotCallEnrollmentServiceWhenTeiServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();
        Gson gson = new Gson();
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(teiService).getTrackedEntityInstances(dhisSyncRequestBody.getService(), mappingJson);
        doNothing().when(loggerService).addLog(service, user, comment, new Date(), new Date());
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doThrow(new SyncFailedException("instance sync failed")).when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (HttpServerErrorException e) {
            verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
            verify(teiService, times(1)).getTrackedEntityInstances(
                    getDhisSyncRequestBody().getService(), mappingJson);
            verify(loggerService, times(1)).addLog(service, user, comment, new Date(), new Date());
            verify(loggerService, times(1)).updateLog(service, "failed");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
            verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(0));
            assertEquals("500 SYNC FAILED", e.getMessage());
        }
    }

    @Test
    public void shouldNotCallUpdatedCompletedEnrollmentServiceWhenNewCompletedEnrollmentIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();
        Gson gson = new Gson();
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment, startDate, endDate);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).getTrackedEntityInstances(dhisSyncRequestBody.getService(), mappingJson);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
        doThrow(new SyncFailedException("instance sync failed")).when(completedEnrollmentService)

                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
        doNothing().when(teiService).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service, getDate(startDate), getDate(endDate));

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (HttpServerErrorException e) {
            verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
            verify(loggerService, times(1)).addLog(service, user, comment, startDate, endDate);
            verify(loggerService, times(1)).updateLog(service, "failed");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).getTrackedEntityInstances(
                    getDhisSyncRequestBody().getService(),
                    mappingJson
            );
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
            verify(completedEnrollmentService, times(1))
                    .triggerJobForNewCompletedEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), anyString(),anyString(),anyString());
            verify(completedEnrollmentService, times(0)).triggerJobForUpdatedCompletedEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), any(), anyString(),anyString(), anyString());
            verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(1));
            TrackersHandler.clearTrackerLists();
            verify(teiService, times(1)).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service, getDate(startDate), getDate(endDate));
            assertEquals("500 SYNC FAILED", e.getMessage());
        }
    }

    @Test
    public void shouldNotCallUpdatedCancelledEnrollmentServiceWhenNewCancelledEnrollmentIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();
        Gson gson = new Gson();
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment, startDate, endDate);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).getTrackedEntityInstances(dhisSyncRequestBody.getService(), mappingJson);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
        doThrow(new SyncFailedException("instance sync failed")).when(cancelledEnrollmentService)
                .triggerJobForNewCancelledEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), anyString(),anyString(), anyString());
        doNothing().when(teiService).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service,getDate(startDate), getDate(endDate));

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (HttpServerErrorException e) {
            verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
            verify(loggerService, times(1)).addLog(service, user, comment, startDate, endDate);
            verify(loggerService, times(1)).updateLog(service, "failed");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).getTrackedEntityInstances(
                    getDhisSyncRequestBody().getService(),
                    mappingJson
            );
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
            verify(cancelledEnrollmentService, times(1))
                    .triggerJobForNewCancelledEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), anyString(),anyString(), anyString());
            verify(cancelledEnrollmentService, times(0)).triggerJobForUpdatedCancelledEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), any(), anyString(),anyString(), anyString());
            verify(markerUtil, times(1)).getLastSyncedDate(service, "enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(1));
            TrackersHandler.clearTrackerLists();
            verify(teiService, times(1)).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service, getDate(startDate), getDate(endDate));
            assertEquals("500 SYNC FAILED", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWithNoDataToSync() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();
        Gson gson = new Gson();
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        doNothing().when(teiService).getTrackedEntityInstances(getDhisSyncRequestBody().getService(), mappingJson);
        doNothing().when(loggerService).addLog(service, user, comment, startDate, endDate);
        doNothing().when(loggerService).updateLog(service, "success");
        doNothing().when(loggerService).collateLogMessage("No delta data to sync.");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
        doNothing().when(completedEnrollmentService).triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
        doNothing().when(completedEnrollmentService).triggerJobForUpdatedCompletedEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), any(), anyString(), anyString(), anyString());


        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (Exception e) {
            verify(loggerService, times(1)).addLog(service, user, comment, startDate, endDate);
            verify(teiService, times(1)).getTrackedEntityInstances(
                    getDhisSyncRequestBody().getService(),
                    mappingJson);
            verify(loggerService, times(1)).updateLog(service, "success");
            verify(loggerService, times(1)).collateLogMessage("No delta data to sync.");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
            verify(completedEnrollmentService, times(1)).triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
            verify(markerUtil, times(1)).getLastSyncedDate(service, "new_active_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "new_completed_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "updated_active_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "updated_completed_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(6));
            TrackersHandler.clearTrackerLists();
            verify(teiService, times(1)).getEnrollmentsForInstances("hts_program_enrollment_table", "hts_program_events_table", service, getDate(startDate), getDate(endDate));

            assertEquals("500 NO DATA TO SYNC", e.getMessage());
        }
    }

    @Test
    public void shouldNotCallActiveEnrollmentServiceWhenCompletedEnrollmentServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment, startDate, endDate);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
        doNothing().when(completedEnrollmentService)
                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
        doNothing().when(completedEnrollmentService)
                .triggerJobForUpdatedCompletedEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), any(), anyString(), anyString(), anyString());

        doThrow(new SyncFailedException("instance sync failed")).when(completedEnrollmentService)

                .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (HttpServerErrorException e) {
            verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
            verify(loggerService, times(1)).addLog(service, user, comment, startDate, endDate);
            verify(loggerService, times(1)).updateLog(service, "failed");
            verify(mappingService, times(1)).getMapping(service);
            verify(completedEnrollmentService, times(1))
             .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
            verify(activeEnrollmentService, times(0))
                    .triggerJobForNewActiveEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());

            verify(markerUtil, times(1)).getLastSyncedDate(service, "new_active_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "new_completed_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "updated_active_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "updated_completed_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(1));
            assertEquals("500 SYNC FAILED", e.getMessage());
        }
    }

    @Test
    public void shouldNotInvokeSecondJobOfActiveEnrollmentServiceIfFirstJobFails() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(dhisMetaDataService).filterByTypeDateTime();
        doNothing().when(loggerService).addLog(service, user, comment, startDate, endDate);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any(), anyList(), anyList(), anyString(), anyString());
        doNothing().when(completedEnrollmentService)
                      .triggerJobForNewCompletedEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
        doNothing().when(completedEnrollmentService)
                .triggerJobForUpdatedCompletedEnrollments(anyString(),anyString(), anyString(), anyString(), anyString(), any(), any(), anyString(), anyString(), anyString());

        doThrow(new SyncFailedException("instance sync failed")).when(activeEnrollmentService)
                .triggerJobForNewActiveEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());


        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (HttpServerErrorException e) {
            verify(dhisMetaDataService, times(1)).filterByTypeDateTime();
            verify(loggerService, times(1)).addLog(service, user, comment, startDate, endDate);
            verify(loggerService, times(1)).updateLog(service, "failed");
            verify(mappingService, times(1)).getMapping(service);
            verify(activeEnrollmentService, times(1))

                    .triggerJobForNewActiveEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), anyString(), anyString(), anyString());
            verify(activeEnrollmentService, times(0))
                    .triggerJobForUpdatedActiveEnrollments(anyString(), anyString(), anyString(), anyString(), anyString(), any(), any(), anyString(), anyString(), anyString());

            verify(markerUtil, times(1)).getLastSyncedDate(service, "new_active_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "new_completed_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "updated_active_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "updated_completed_enrollment");
            verify(markerUtil, times(1)).getLastSyncedDate(service, "event");
            verifyStatic(times(3));
            assertEquals("500 SYNC FAILED", e.getMessage());
        }
    }


    private DHISSyncRequestBody getDhisSyncRequestBody() {
        DHISSyncRequestBody dhisSyncRequestBody = new DHISSyncRequestBody();
        dhisSyncRequestBody.setService(service);
        dhisSyncRequestBody.setUser(user);
        dhisSyncRequestBody.setComment(comment);
        dhisSyncRequestBody.setStartDate(startDate);
        dhisSyncRequestBody.setEndDate(endDate);
        return dhisSyncRequestBody;
    }

    private String getDate(Date date) {
        return getStringFromDate(date, "yyyy-MM-dd");
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
                "\"comparable\":[" +
                "Patient_Identifier" +
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
