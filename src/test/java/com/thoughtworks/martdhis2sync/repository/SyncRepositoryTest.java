package com.thoughtworks.martdhis2sync.repository;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;
import java.util.Collections;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class SyncRepositoryTest {
    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ResponseEntity<DHISSyncResponse> responseEntity;

    @Mock
    private ResponseEntity<OrgUnitResponse> orgUnitResponse;

    @Mock
    private ResponseEntity<DataElementResponse> dataElementResponse;

    @Mock
    private Logger logger;

    @Mock
    private LoggerService loggerService;

    @Mock
    private RestClientException restClientException;

    @Mock
    private ResponseEntity<TrackedEntityAttributeResponse> trackedEntityAttributeResposne;

    @Mock
    private ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstanceResponse;

    private SyncRepository syncRepository;
    private String body = "{" +
            "\"trackedEntityType\": \"o0kaqrZaY\", " +
            "\"trackedEntityInstance\": \"EmACSYDCxhu\", " +
            "\"orgUnit\":\"SxgCPPeiq3c\", " +
            "\"attributes\":" +
            "[" +
            "{" +
            "\"attribute\": \"rOb34aQLSyC\", " +
            "\"value\": \"UIC00014\"" +
            "}" +
            "]" +
            "}";

    @Before
    public void setUp() throws Exception {
        syncRepository = new SyncRepository();
        setValuesForMemberFields(syncRepository, "logger", logger);
        setValuesForMemberFields(syncRepository, "restTemplate", restTemplate);
        setValuesForMemberFields(syncRepository, "loggerService", loggerService);
    }

    @Test
    public void shouldReturnResponseEntityAndCallLoggerInfo() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        doNothing().when(logger).info("SyncRepository: Received 200 status code.");

        ResponseEntity<DHISSyncResponse> actualResponse = syncRepository.sendData("/api/trackedEntityInstance", body);

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(responseEntity, times(1)).getStatusCode();
        verify(logger, times(1)).info("SyncRepository: Received 200 status code.");

        assertEquals(responseEntity, actualResponse);
    }

    @Test
    public void shouldReturnResponseEntityAndCallLoggerErrorAndLoggerServiceToCollateMessage() {
        String response = "{" +
                "\"httpStatus\":\"Conflict\", " +
                "\"httpStatusCode\":\"409\", " +
                "\"status\":\"ERROR\", " +
                "\"response\":{" +
                "\"ignored\":1, " +
                "\"importSummaries\": [" +
                "{\"description\": \"Program has another active enrollment going on. Not possible to incomplete\"}" +
                "]" +
                "}" +
                "}";

        ImportSummary importSummary = new ImportSummary();
        importSummary.setDescription("Program has another active enrollment going on. Not possible to incomplete");
        Response responseObj = new Response();
        responseObj.setIgnored(1);
        responseObj.setImportSummaries(Collections.singletonList(importSummary));
        DHISSyncResponse dhisSyncResponse = new DHISSyncResponse();
        dhisSyncResponse.setHttpStatus("Conflict");
        dhisSyncResponse.setHttpStatusCode(409);
        dhisSyncResponse.setStatus("ERROR");
        dhisSyncResponse.setResponse(responseObj);
        ResponseEntity<DHISSyncResponse> errorResponse = new ResponseEntity<>(dhisSyncResponse, HttpStatus.CONFLICT);
        Charset charset = Charset.forName("UTF-8");

        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.CONFLICT, "CONFLICT", response.getBytes(), charset));
        doNothing().when(logger).error("SyncRepository: org.springframework.web.client.HttpClientErrorException: 409 CONFLICT");
        doNothing().when(loggerService).collateLogMessage("409 CONFLICT");

        ResponseEntity<DHISSyncResponse> actualResponse = syncRepository.sendData("/api/trackedEntityInstance", body);

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(logger, times(1)).error("SyncRepository: org.springframework.web.client.HttpClientErrorException: 409 CONFLICT");
        verify(loggerService, times(1)).collateLogMessage("409 CONFLICT");

        assertEquals(errorResponse, actualResponse);
    }

    @Test
    public void shouldThrowExceptionAndCallLoggerErrorAndLoggerServiceToCollateMessage() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(new HttpServerErrorException(HttpStatus.CONFLICT, "CONFLICT"));
        doNothing().when(logger).error("SyncRepository: org.springframework.web.client.HttpServerErrorException: 409 CONFLICT");
        doNothing().when(loggerService).collateLogMessage("409 CONFLICT");

        try {
            syncRepository.sendData("/api/trackedEntityInstance", body);
        } catch (HttpServerErrorException e) {
            verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
            verify(logger, times(1)).error("SyncRepository: org.springframework.web.client.HttpServerErrorException: 409 CONFLICT");
            verify(loggerService, times(1)).collateLogMessage("409 CONFLICT");
        }
    }

    @Test
    public void shouldReturnOrgUnitsAndLogTheInfo() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(orgUnitResponse);
        when(orgUnitResponse.getStatusCode()).thenReturn(HttpStatus.OK);
        doNothing().when(logger).info("SyncRepository: Received 200 status code.");

        ResponseEntity<OrgUnitResponse> orgUnits = syncRepository.getOrgUnits("");

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(orgUnitResponse, times(1)).getStatusCode();
        verify(logger, times(1)).info("SyncRepository: Received 200 status code.");

        assertEquals(orgUnitResponse, orgUnits);
    }

    @Test
    public void shouldGetDataElementsInfoAndLog() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(dataElementResponse);
        when(dataElementResponse.getStatusCode()).thenReturn(HttpStatus.OK);
        doNothing().when(logger).info("SyncRepository: Received 200 status code.");

        ResponseEntity<DataElementResponse> dataElements = syncRepository.getDataElements("");

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(dataElementResponse, times(1)).getStatusCode();
        verify(logger, times(1)).info("SyncRepository: Received 200 status code.");

        assertEquals(dataElementResponse, dataElements);
    }

    @Test
    public void shouldGetTEAttributesInfoAndLog() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(trackedEntityAttributeResposne);
        when(trackedEntityAttributeResposne.getStatusCode()).thenReturn(HttpStatus.OK);
        doNothing().when(logger).info("SyncRepository: Received 200 status code.");

        ResponseEntity<TrackedEntityAttributeResponse> trackedEntityAttributes = syncRepository.getTrackedEntityAttributes("");

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(trackedEntityAttributes, times(1)).getStatusCode();
        verify(logger, times(1)).info("SyncRepository: Received 200 status code.");

        assertEquals(trackedEntityAttributeResposne, trackedEntityAttributes);
    }

    @Test
    public void shouldGetTEIsInfoAndLog() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenReturn(trackedEntityInstanceResponse);
        when(trackedEntityInstanceResponse.getStatusCode()).thenReturn(HttpStatus.OK);
        doNothing().when(logger).info("SyncRepository: Received 200 status code.");

        ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstances = syncRepository.getTrackedEntityInstances("");

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(trackedEntityInstances, times(1)).getStatusCode();
        verify(logger, times(1)).info("SyncRepository: Received 200 status code.");

        assertEquals(trackedEntityInstanceResponse, trackedEntityInstances);
    }

    @Test
    public void shouldThrowExceptionWhenGettingDataElementsInfoAndLogThat() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(restClientException);
        doNothing().when(logger).info("SyncRepository: org.springframework.web.client.HttpServerErrorException: 409 CONFLICT");

        try {
            syncRepository.getDataElements("");
        } catch (RestClientException r) {
            verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
            verify(logger, times(1)).info("SyncRepository: org.springframework.web.client.HttpServerErrorException: 409 CONFLICT");
        }
    }

    @Test
    public void shouldReturnNullAndLogTheError() {
        when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenThrow(new HttpServerErrorException(HttpStatus.CONFLICT, "CONFLICT"));
        doNothing().when(logger).error("SyncRepository: org.springframework.web.client.HttpServerErrorException: 409 CONFLICT");

        ResponseEntity<OrgUnitResponse> orgUnits = syncRepository.getOrgUnits("/api/TrackedEntityInstance");

        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), any(Class.class));
        verify(logger, times(1)).error("SyncRepository: org.springframework.web.client.HttpServerErrorException: 409 CONFLICT");

        assertNull(orgUnits);
    }
}
