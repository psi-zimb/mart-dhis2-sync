package com.thoughtworks.martdhis2sync.responseHandler;

import com.thoughtworks.martdhis2sync.model.Conflict;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.Conflict.CONFLICT_OBJ_ENROLLMENT_DATE;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
public class EnrollmentResponseHandlerTest {
    @Mock
    private LoggerService loggerService;

    @Mock
    private Logger logger;

    private EnrollmentResponseHandler responseHandler;

    private EnrollmentAPIPayLoad payLoad1;
    private EnrollmentAPIPayLoad payLoad2;
    private EnrollmentAPIPayLoad payLoad3;

    @Before
    public void setUp() throws Exception {
        String instanceId1 = "instance1";
        String instanceId2 = "instance2";
        String instanceId3 = "instance3";
        String enrDate = "2018-10-13";

        responseHandler = new EnrollmentResponseHandler();

        setValuesForMemberFields(responseHandler, "loggerService", loggerService);

        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, "1");
        payLoad2 = getEnrollmentPayLoad(instanceId2, enrDate, "2");
        payLoad3 = getEnrollmentPayLoad(instanceId3, enrDate, "3");
    }

    @Test
    public void shouldAddEnrollmentsToEnrollmentsToSaveInTrackerWithUid() {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        String enrReference1 = "enrReference1";
        String enrReference2 = "enrReference2";
        String enrReference3 = "enrReference3";
        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference2, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, null
                )
        );
        payLoad1.setEnrollmentId(enrReference1);
        payLoad2.setEnrollmentId(enrReference2);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> expectedEnrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad2, payLoad3);
        Iterator<EnrollmentAPIPayLoad> iterator = expectedEnrollmentsToSaveInTracker.iterator();

        responseHandler.processImportSummaries(importSummaries, iterator);

        assertEquals(expectedEnrollmentsToSaveInTracker, EnrollmentUtil.enrollmentsToSaveInTracker);
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldAddOnlySuccessfullySyncedEnrollmentsToEnrollmentsToSaveInTrackerWithUID() {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        String logPrefix = "New Completed Enrollments:: ";
        String enrReference1 = "enrReference1";
        String enrReference3 = "enrReference3";

        String conflictMessage = "Enrollment Date can't be future date :Mon Oct 20 00:00:00 IST 2020";
        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        null, Collections.singletonList(new Conflict(CONFLICT_OBJ_ENROLLMENT_DATE, conflictMessage)),
                        null, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, null
                )
        );
        payLoad1.setEnrollmentId(enrReference1);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> payLoads = Arrays.asList(payLoad1, payLoad2, payLoad3);
        Iterator<EnrollmentAPIPayLoad> iterator = payLoads.iterator();

        responseHandler.processErrorResponse(importSummaries, iterator, logger, logPrefix);

        verify(logger, times(1)).error(logPrefix + CONFLICT_OBJ_ENROLLMENT_DATE + ": " +
                conflictMessage);
        verify(loggerService, times(1)).collateLogMessage(CONFLICT_OBJ_ENROLLMENT_DATE + ": " +
                conflictMessage);
        assertEquals(Arrays.asList(payLoad1, payLoad3), EnrollmentUtil.enrollmentsToSaveInTracker);

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldLogTheMessageAndAddToTrackerForSuccessfullySyncedEnrollments() {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        String logPrefix = "New Completed Enrollments:: ";
        String enrReference2 = "enrReference2";
        String enrReference3 = "enrReference3";

        String description = "TrackedEntityInstance instance1 already has an active enrollment in program xhjKKwoq";
        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        description, new ArrayList<>(), null, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference2, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, null
                )
        );
        payLoad2.setEnrollmentId(enrReference2);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> payLoads = Arrays.asList(payLoad1, payLoad2, payLoad3);
        Iterator<EnrollmentAPIPayLoad> iterator = payLoads.iterator();

        responseHandler.processErrorResponse(importSummaries, iterator, logger, logPrefix);

        verify(logger, times(1)).error(logPrefix + description);
        verify(loggerService, times(1)).collateLogMessage(description);
        assertEquals(Arrays.asList(payLoad2, payLoad3), EnrollmentUtil.enrollmentsToSaveInTracker);

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, String programUniqueId) {
        return new EnrollmentAPIPayLoad(
                "",
                instanceId,
                "xhjKKwoq",
                "jSsoNjesL",
                enrDate,
                enrDate,
                "ACTIVE",
                programUniqueId,
                Collections.emptyList()
        );
    }
}
