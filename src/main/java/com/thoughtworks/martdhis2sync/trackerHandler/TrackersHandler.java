package com.thoughtworks.martdhis2sync.trackerHandler;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.GetUTCDateTimeAsString;

@Component
public class TrackersHandler {

    @Autowired
    private DataSource dataSource;

    public int insertInEnrollmentTracker(String user) throws SQLException {
        String sql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                updateCount = 0;
                for (EnrollmentAPIPayLoad enrollment : EnrollmentUtil.enrollmentsToSaveInTracker) {
                    ps.setString(1, enrollment.getEnrollmentId());
                    ps.setString(2, enrollment.getInstanceId());
                    ps.setString(3, enrollment.getProgram());
                    ps.setString(4, enrollment.getStatus());
                    ps.setString(5, enrollment.getProgramUniqueId());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    public int insertInEventTracker(String user) throws SQLException {
        String sql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                updateCount = 0;
                for (EventTracker eventTracker : EventUtil.eventsToSaveInTracker) {
                    ps.setString(1, eventTracker.getEventId());
                    ps.setString(2, eventTracker.getInstanceId());
                    ps.setString(3, eventTracker.getProgram());
                    ps.setString(4, eventTracker.getProgramStage());
                    ps.setString(5, eventTracker.getEventUniqueId());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    public int updateInEnrollmentTracker(String user) throws SQLException {
        String sql = "UPDATE public.enrollment_tracker " +
                "SET status = ?, created_by = ?, date_created = ? " +
                "WHERE enrollment_id = ?";
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                updateCount = 0;
                for (EnrollmentAPIPayLoad enrollment : EnrollmentUtil.enrollmentsToSaveInTracker) {
                    ps.setString(1, enrollment.getStatus());
                    ps.setString(2, user);
                    ps.setTimestamp(3, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    ps.setString(4, enrollment.getEnrollmentId());
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }
}