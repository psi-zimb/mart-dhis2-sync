package com.thoughtworks.martdhis2sync.controller;

import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.service.EventService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.service.MappingService;
import com.thoughtworks.martdhis2sync.service.ProgramEnrollmentService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.SyncFailedException;
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
    private ProgramEnrollmentService programEnrollmentService;

    @Mock
    private EventService eventService;

    @Mock
    private LoggerService loggerService;

    private PushController pushController;
    private String service = "HT Service";
    private String user = "***REMOVED***";
    private String comment = "";

    @Before
    public void setUp() throws Exception {
        pushController = new PushController();
        setValuesForMemberFields(pushController, "mappingService", mappingService);
        setValuesForMemberFields(pushController, "teiService", teiService);
        setValuesForMemberFields(pushController, "programEnrollmentService", programEnrollmentService);
        setValuesForMemberFields(pushController, "eventService", eventService);
        setValuesForMemberFields(pushController, "loggerService", loggerService);
    }

    @Test
    public void shouldNotCallEnrollmentAndEventServiceWhenTeiServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doThrow(new SyncFailedException("instance sync failed")).when(teiService).triggerJob(anyString(), anyString(), anyString(), any());

        pushController.pushData(dhisSyncRequestBody);

        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
        verify(programEnrollmentService, times(0)).triggerJob(service, user, "hts_program_enrollment_table");
        verify(eventService, times(0)).triggerJob(anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    public void shouldNotCallEventServiceWhenEnrollmentServiceIsFailed() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any());
        doThrow(new SyncFailedException("enrollment sync failed")).when(programEnrollmentService).triggerJob(service, user, "hts_program_enrollment_table");

        pushController.pushData(dhisSyncRequestBody);

        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
        verify(programEnrollmentService, times(1)).triggerJob(service, user, "hts_program_enrollment_table");
        verify(eventService, times(0)).triggerJob(anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    public void shouldCallLoggerUpdateWithStatusAsFailedOnEventServiceFail() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "failed");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any());
        doNothing().when(programEnrollmentService).triggerJob(service, user, "hts_program_enrollment_table");
        doThrow(new SyncFailedException("event sync failed")).when(eventService).triggerJob(anyString(), anyString(), anyString(), any(), anyString());

        pushController.pushData(dhisSyncRequestBody);

        verify(loggerService, times(1)).addLog(service, user, comment);
        verify(loggerService, times(1)).updateLog(service, "failed");
        verify(mappingService, times(1)).getMapping(service);
        verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
        verify(programEnrollmentService, times(1)).triggerJob(service, user, "hts_program_enrollment_table");
        verify(eventService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    public void shouldThrowExceptionWithNoDataToSync() throws Exception {
        Map<String, Object> mapping = getMapping();
        DHISSyncRequestBody dhisSyncRequestBody = getDhisSyncRequestBody();

        doNothing().when(loggerService).addLog(service, user, comment);
        doNothing().when(loggerService).updateLog(service, "success");
        doNothing().when(loggerService).collateLogMessage("No delta data to sync.");
        when(mappingService.getMapping(service)).thenReturn(mapping);
        doNothing().when(teiService).triggerJob(anyString(), anyString(), anyString(), any());
        doNothing().when(programEnrollmentService).triggerJob(service, user, "hts_program_enrollment_table");
        doNothing().when(programEnrollmentService).triggerJob(service, user, "hts_program_enrollment_table");

        try {
            pushController.pushData(dhisSyncRequestBody);
        } catch (Exception e ) {
            verify(loggerService, times(1)).addLog(service, user, comment);
            verify(loggerService, times(1)).updateLog(service, "success");
            verify(loggerService, times(1)).collateLogMessage("No delta data to sync.");
            verify(mappingService, times(1)).getMapping(service);
            verify(teiService, times(1)).triggerJob(anyString(), anyString(), anyString(), any());
            verify(programEnrollmentService, times(1)).triggerJob(service, user, "hts_program_enrollment_table");
            verify(eventService, times(1)).triggerJob(anyString(), anyString(), anyString(), any(), anyString());

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
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("mapping_name", service);
        mapping.put("lookup_table", lookupTable);
        mapping.put("mapping_json", mappingJson);
        return mapping;
    }
}