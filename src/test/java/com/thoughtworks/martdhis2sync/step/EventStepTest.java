package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.EventProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.writer.EventWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.ObjectFactory;

import java.util.Date;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_EVENT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(EventUtil.class)
@PowerMockIgnore("javax.management.*")
public class EventStepTest {
    @Mock
    private MappingReader mappingReader;

    @Mock
    private JdbcCursorItemReader<Map<String, Object>> jdbcCursorItemReader;

    @Mock
    private ObjectFactory<EventProcessor> objectFactory;

    @Mock
    private EventProcessor processor;

    @Mock
    private EventWriter writer;

    @Mock
    private TaskletStep step;

    @Mock
    private StepFactory stepFactory;

    @Mock
    private MarkerUtil markerUtil;

    private EventStep eventStep;

    @Before
    public void setUp() throws Exception {
        eventStep = new EventStep();
        eventStep.setEnrollmentLookupTable("patient_enrollment");
        setValuesForMemberFields(eventStep, "mappingReader", mappingReader);
        setValuesForMemberFields(eventStep, "processorObjectFactory", objectFactory);
        setValuesForMemberFields(eventStep, "writer", writer);
        setValuesForMemberFields(eventStep, "stepFactory", stepFactory);
        setValuesForMemberFields(eventStep, "markerUtil", markerUtil);
    }

    @Test
    public void shouldReturnStep() {
        String lookupTable = "patient_identifier";
        String programName = "Enrollment Service";
        String stepName = "Event Step";
        String mappingObj = "";
        Date lastSyncedDate = new Date(Long.MIN_VALUE);
        String enrollmentLookupTable = "patient_enrollment";

        mockStatic(EventUtil.class);
        when(mappingReader.getEventReader(lookupTable, programName, enrollmentLookupTable)).thenReturn(jdbcCursorItemReader);
        when(objectFactory.getObject()).thenReturn(processor);
        when(stepFactory.build(stepName, jdbcCursorItemReader, processor, writer)).thenReturn(step);
        when(markerUtil.getLastSyncedDate(programName, CATEGORY_EVENT)).thenReturn(lastSyncedDate);


        Step actual = eventStep.get(lookupTable, programName, mappingObj);

        verifyStatic(times(1));
        EventUtil.resetEventTrackersList();
        verify(mappingReader, times(1)).getEventReader(lookupTable, programName, enrollmentLookupTable);
        verify(stepFactory, times(1)).build(stepName, jdbcCursorItemReader, processor, writer);
        verify(markerUtil, times(1)).getLastSyncedDate(programName, CATEGORY_EVENT);
        assertEquals(step, actual);
    }
}
