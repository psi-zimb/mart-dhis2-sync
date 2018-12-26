package com.thoughtworks.martdhis2sync.trackerHandler;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class TrackersHandlerTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Logger logger;

    private TrackersHandler trackersHandler;
    private String logPrefix = "Test Class: ";

    @Before
    public void setUp() throws Exception {
        trackersHandler = new TrackersHandler();

        setValuesForMemberFields(trackersHandler, "dataSource", dataSource);
    }

    @Test
    public void shouldInsertEnrollmentsIntoEnrollmentTracker() throws SQLException {
        String sql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";
        EnrollmentAPIPayLoad apiPayLoad = new EnrollmentAPIPayLoad(
                "enrId",
                "instanceId",
                "xhjKKwoq",
                "jSsoNjesL",
                "2019-10-12",
                "2019-10-12",
                "COMPLETED",
                "1",
                null
        );
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        EnrollmentUtil.enrollmentsToSaveInTracker.add(apiPayLoad);

        trackersHandler.insertInEnrollmentTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(preparedStatement, times(1)).setString(1, "enrId");
        verify(preparedStatement, times(1)).setString(2, "instanceId");
        verify(preparedStatement, times(1)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "COMPLETED");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(6, "***REMOVED***");
        verify(preparedStatement, times(1)).executeUpdate();
        verify(logger, times(1)).info(logPrefix + "Successfully inserted 1 Enrollment UIDs.");

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldLogErrorWhenInsertIsFail() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Could not get Database connection"));

        trackersHandler.insertInEnrollmentTracker("***REMOVED***", logPrefix, logger);
        verify(dataSource, times(1)).getConnection();
        verify(logger, times(1)).error(logPrefix + "Exception occurred while inserting " +
                    "Program Enrollment UIDs: Could not get Database connection");

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldNotInsertEnrollmentsIntoEnrTrackerIfPrepareStatementIsFailed() throws SQLException {
        String sql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenThrow(new SQLException("Could not prepareStatement"));

        trackersHandler.insertInEnrollmentTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(logger, times(1)).error(logPrefix + "Exception occurred while inserting Program " +
                "Enrollment UIDs: Could not prepareStatement");
    }

    @Test
    public void shouldNotInsertEnrollmentsIntoEnrollmentTrackerWhenExecuteUpdateIsFailed() throws SQLException {
        String sql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        EnrollmentAPIPayLoad apiPayLoad = new EnrollmentAPIPayLoad(
                "enrId",
                "instanceId",
                "xhjKKwoq",
                "jSsoNjesL",
                "2019-10-12",
                "2019-10-12",
                "COMPLETED",
                "1",
                null
        );

        EnrollmentUtil.enrollmentsToSaveInTracker.add(apiPayLoad);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Could not execute update"));

        trackersHandler.insertInEnrollmentTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(preparedStatement, times(1)).setString(1, "enrId");
        verify(preparedStatement, times(1)).setString(2, "instanceId");
        verify(preparedStatement, times(1)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "COMPLETED");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(6, "***REMOVED***");
        verify(preparedStatement, times(1)).executeUpdate();
        verify(logger, times(1)).error(logPrefix + "Exception occurred while inserting " +
                "Program Enrollment UIDs: Could not execute update");
    }

    @Test
    public void shouldInsertEventIntoEventTracker() throws SQLException {
        String sql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        List<EventTracker> eventTrackers = new LinkedList<>();
        eventTrackers.add(new EventTracker("eventId1", "instanceId1", "xhjKKwoq", "1", "dkjfErjA"));
        eventTrackers.add(new EventTracker("eventId2", "instanceId1", "xhjKKwoq", "2", "dkjfErjA"));
        EventUtil.eventsToSaveInTracker = eventTrackers;

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        trackersHandler.insertInEventTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(preparedStatement, times(1)).setString(1, "eventId1");
        verify(preparedStatement, times(1)).setString(1, "eventId2");
        verify(preparedStatement, times(2)).setString(2, "instanceId1");
        verify(preparedStatement, times(2)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(2)).setString(4, "dkjfErjA");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(5, "2");
        verify(preparedStatement, times(2)).setString(6, "***REMOVED***");
        verify(preparedStatement, times(2)).executeUpdate();
        verify(logger, times(1)).info(logPrefix + "Successfully inserted 2 Event UIDs.");
    }

    @Test
    public void shouldNotInsertEventsIntoEventTrackerIfThereIsError() throws SQLException {
        String sql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenThrow(new SQLException("Could not prepareStatement"));

        trackersHandler.insertInEventTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(logger, times(1)).error(logPrefix + "Exception occurred while inserting Event " +
                "UIDs: Could not prepareStatement");
    }

    @Test
    public void shouldNoInsertEventsIntoEventTrackerWhenGettingConnectionIsFailed() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Could not get Database connection"));

        trackersHandler.insertInEventTracker("***REMOVED***", logPrefix, logger);
        verify(dataSource, times(1)).getConnection();
        verify(logger, times(1)).error(logPrefix + "Exception occurred while inserting " +
                "Event UIDs: Could not get Database connection");

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldNotInsertEventsIntoEventsTrackerWhenExecuteUpdateIsFailed() throws SQLException {
        String sql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        List<EventTracker> eventTrackers = new LinkedList<>();
        eventTrackers.add(new EventTracker("eventId1", "instanceId1", "xhjKKwoq", "1", "dkjfErjA"));
        eventTrackers.add(new EventTracker("eventId2", "instanceId1", "xhjKKwoq", "2", "dkjfErjA"));
        EventUtil.eventsToSaveInTracker = eventTrackers;

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Could not execute update"));

        trackersHandler.insertInEventTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(preparedStatement, times(1)).setString(1, "eventId1");
        verify(preparedStatement, times(1)).setString(2, "instanceId1");
        verify(preparedStatement, times(1)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "dkjfErjA");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(6, "***REMOVED***");
        verify(preparedStatement, times(1)).executeUpdate();
        verify(logger, times(1)).error(logPrefix + "Exception occurred while inserting " +
                "Event UIDs: Could not execute update");
    }

    @Test
    public void shouldUpdateEnrollmentSInEnrollmentTracker() throws SQLException {
        String sql = "UPDATE public.enrollment_tracker " +
                "SET status = ?, created_by = ?, date_created = ? " +
                "WHERE enrollment_id = ?";
        EnrollmentAPIPayLoad apiPayLoad = new EnrollmentAPIPayLoad(
                "enrId",
                "instanceId",
                "xhjKKwoq",
                "jSsoNjesL",
                "2019-10-12",
                "2019-10-12",
                "COMPLETED",
                "1",
                null
        );
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        LinkedList<EnrollmentAPIPayLoad> apiPayLoads = new LinkedList<>();
        apiPayLoads.add(apiPayLoad);
        EnrollmentUtil.enrollmentsToSaveInTracker = apiPayLoads;

        trackersHandler.updateInEnrollmentTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(preparedStatement, times(1)).setString(1, "COMPLETED");
        verify(preparedStatement, times(1)).setString(2, "***REMOVED***");
        verify(preparedStatement, times(1)).setString(4, "enrId");
        verify(preparedStatement, times(1)).executeUpdate();
        verify(logger, times(1)).info(logPrefix + "Successfully updated 1 Enrollment UIDs.");
    }

    @Test
    public void shouldNotUpdateEnrollmentsInEnrollmentTrackerIfThereIsError() throws SQLException {
        String sql = "UPDATE public.enrollment_tracker " +
                "SET status = ?, created_by = ?, date_created = ? " +
                "WHERE enrollment_id = ?";
        EnrollmentAPIPayLoad apiPayLoad = new EnrollmentAPIPayLoad(
                "enrId",
                "instanceId",
                "xhjKKwoq",
                "jSsoNjesL",
                "2019-10-12",
                "2019-10-12",
                "COMPLETED",
                "1",
                null
        );

        LinkedList<EnrollmentAPIPayLoad> apiPayLoads = new LinkedList<>();
        apiPayLoads.add(apiPayLoad);
        EnrollmentUtil.enrollmentsToSaveInTracker = apiPayLoads;

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Could not execute update"));

        trackersHandler.updateInEnrollmentTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(preparedStatement, times(1)).setString(1, "COMPLETED");
        verify(preparedStatement, times(1)).setString(2, "***REMOVED***");
        verify(preparedStatement, times(1)).setString(4, "enrId");
        verify(preparedStatement, times(1)).executeUpdate();
        verify(logger, times(1)).error(logPrefix + "Exception occurred while updating " +
                "Program Enrollment UIDs: Could not execute update");
    }

    @Test
    public void shouldNoUpdateEnrollmentsIntoEnrollmentTrackerWhenGettingConnectionIsFailed() throws SQLException {
        when(dataSource.getConnection()).thenThrow(new SQLException("Could not get Database connection"));

        trackersHandler.updateInEnrollmentTracker("***REMOVED***", logPrefix, logger);
        verify(dataSource, times(1)).getConnection();
        verify(logger, times(1)).error(logPrefix + "Exception occurred while updating " +
                "Program Enrollment UIDs: Could not get Database connection");
    }

    @Test
    public void shouldNotUpdateEnrollmentsIntoEnrTrackerIfPrepareStatementIsFailed() throws SQLException {
        String sql = "UPDATE public.enrollment_tracker " +
                "SET status = ?, created_by = ?, date_created = ? " +
                "WHERE enrollment_id = ?";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sql)).thenThrow(new SQLException("Could not prepareStatement"));

        trackersHandler.updateInEnrollmentTracker("***REMOVED***", logPrefix, logger);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(sql);
        verify(logger, times(1)).error(logPrefix + "Exception occurred while updating Program " +
                "Enrollment UIDs: Could not prepareStatement");
    }

    @Test
    public void shouldClearTrackers() {
        EnrollmentAPIPayLoad apiPayLoad = new EnrollmentAPIPayLoad(
                "enrId",
                "instanceId",
                "xhjKKwoq",
                "jSsoNjesL",
                "2019-10-12",
                "2019-10-12",
                "COMPLETED",
                "1",
                null
        );

        LinkedList<EnrollmentAPIPayLoad> apiPayLoads = new LinkedList<>();
        apiPayLoads.add(apiPayLoad);
        EnrollmentUtil.enrollmentsToSaveInTracker = apiPayLoads;

        List<EventTracker> eventTrackers = new LinkedList<>();
        eventTrackers.add(new EventTracker("eventId1", "instanceId1", "xhjKKwoq", "1", "dkjfErjA"));
        eventTrackers.add(new EventTracker("eventId2", "instanceId1", "xhjKKwoq", "2", "dkjfErjA"));
        EventUtil.eventsToSaveInTracker = eventTrackers;

        TrackersHandler.clearTrackerLists();

        Assert.assertEquals(0, EnrollmentUtil.enrollmentsToSaveInTracker.size());
        Assert.assertEquals(0, EventUtil.eventsToSaveInTracker.size());
    }

    @After
    public void tearDown() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EventUtil.eventsToSaveInTracker.clear();
    }
}
