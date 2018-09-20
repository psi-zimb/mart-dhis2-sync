package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.Enrollment;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.model.ImportSummary.RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;

@Component
@StepScope
public class ProgramEnrollmentWriter implements ItemWriter {

    @Autowired
    private SyncRepository syncRepository;

    @Value("${uri.program.enrollments}")
    private String programEnrollUri;

    @Value("#{jobParameters['user']}")
    private String user;

    @Value("#{jobParameters['service']}")
    private String programName;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private MarkerUtil markerUtil;

    private static final String EMPTY_STRING = "\"\"";
    private static List<Enrollment> newEnrollmentsToSave = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "ENROLLMENT SYNC: ";

    @Override
    public void write(List items) {
        StringBuilder enrollmentApiFormat = new StringBuilder("{\"enrollments\":[");
        items.forEach(item -> enrollmentApiFormat.append(item).append(","));
        enrollmentApiFormat.replace(enrollmentApiFormat.length() - 1, enrollmentApiFormat.length(), "]}");

        ResponseEntity<DHISSyncResponse> responseEntity = syncRepository.sendData(programEnrollUri, enrollmentApiFormat.toString());
        processResponse(responseEntity.getBody().getResponse().getImportSummaries());
        updateTracker();
        updateMarker();
    }

    private void processResponse(List<ImportSummary> importSummaries) {
        newEnrollmentsToSave.clear();
        Iterator<Enrollment> mapIterator = EnrollmentUtil.getEnrollmentsList().iterator();

        importSummaries.forEach(importSummary -> {
            if (isImported(importSummary)) {
                while (mapIterator.hasNext()) {
                    Enrollment enrollment = mapIterator.next();
                    if (EMPTY_STRING.equals(enrollment.getEnrollment_id())) {
                        enrollment.setEnrollment_id(importSummary.getReference());
                        newEnrollmentsToSave.add(enrollment);
                    }
                }
            }
        });
    }

    private boolean isImported(ImportSummary importSummary) {
        return RESPONSE_SUCCESS.equals(importSummary.getStatus()) && importSummary.getImportCount().getImported() == 1;
    }

    private void updateTracker() {

        String sqlQuery = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program_name, program_start_date, status, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        try {
            if (!newEnrollmentsToSave.isEmpty()) {
                int recordsCreated = getUpdateCount(sqlQuery);
                logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " Enrollment UIDs.");
            }
        } catch (SQLException e) {
            logger.error(LOG_PREFIX + "Exception occurred while inserting Program Enrollment UIDs:" + e.getMessage());
            e.printStackTrace();
        }
    }

    private int getUpdateCount(String sqlQuery) throws SQLException {
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                updateCount = 0;
                for (Enrollment enrollment : newEnrollmentsToSave) {
                    ps.setString(1, enrollment.getEnrollment_id());
                    ps.setString(2, enrollment.getInstance_id());
                    ps.setString(3, enrollment.getProgram_name());
                    ps.setDate(4, new Date(enrollment.getProgram_start_date().getTime()));
                    ps.setString(5, enrollment.getStatus());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    private void updateMarker() {
        markerUtil.updateMarkerEntry(programName, "enrollment",
                getStringFromDate(EnrollmentUtil.date, DATEFORMAT_WITH_24HR_TIME));
    }
}
