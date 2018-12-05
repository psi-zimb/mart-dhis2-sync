package com.thoughtworks.martdhis2sync.responseHandler;

import com.thoughtworks.martdhis2sync.model.Enrollment;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.util.EnrollmentUtil.enrollmentsToSaveInTracker;

@Component
public class EnrollmentResponseHandler {

    @Autowired
    private LoggerService loggerService;

    public void processImportSummaries(List<EnrollmentImportSummary> importSummaries, Iterator<EnrollmentAPIPayLoad> payLoadIterator) {
        importSummaries.forEach(importSummary -> {
            EnrollmentAPIPayLoad enrollment = payLoadIterator.next();
            enrollment.setEnrollmentId(importSummary.getReference());
            enrollmentsToSaveInTracker.add(enrollment);
        });
    }

    public void processErrorResponse(List<EnrollmentImportSummary> importSummaries, Iterator<EnrollmentAPIPayLoad> payLoadIterator, Logger logger, String logPrefix) {
        for (EnrollmentImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                payLoadIterator.next();
                logger.error(logPrefix + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                payLoadIterator.next();
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(logPrefix + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
            } else {
                processImportSummaries(Collections.singletonList(importSummary), payLoadIterator);
            }
        }
    }

    public void processCompletedSecondStepResponse(List<EnrollmentImportSummary> importSummaries, Iterator<EnrollmentAPIPayLoad> payLoadIterator, Logger logger, String logPrefix) {
        for (EnrollmentImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                EnrollmentAPIPayLoad payLoad = payLoadIterator.next();
                payLoad.setStatus(Enrollment.STATUS_ACTIVE);
                logger.error(logPrefix + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                EnrollmentAPIPayLoad payLoad = payLoadIterator.next();
                payLoad.setStatus(Enrollment.STATUS_ACTIVE);
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(logPrefix + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
            } else {
                payLoadIterator.next();
            }
        }
    }

    private boolean isIgnored(EnrollmentImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !StringUtils.isEmpty(importSummary.getDescription());
    }

    private boolean isConflicted(EnrollmentImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !importSummary.getConflicts().isEmpty();
    }
}
