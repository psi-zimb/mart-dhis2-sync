package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.NewEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.NewCancelledEnrollmentWithEventsWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.ObjectFactory;

import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class NewCancelledEnrollmentWithEventsStepTest {

    @Mock
    private MappingReader mappingReader;

    @Mock
    private JdbcCursorItemReader<Map<String, Object>> jdbcCursorItemReader;

    @Mock
    private ObjectFactory<NewEnrollmentWithEventsProcessor> objectFactory;

    @Mock
    private NewEnrollmentWithEventsProcessor processor;

    @Mock
    private NewCancelledEnrollmentWithEventsWriter writer;

    @Mock
    private TaskletStep step;

    @Mock
    private StepFactory stepFactory;

    private NewCancelledEnrollmentWithEventsStep eventStep;

    @Before
    public void setUp() throws Exception {
        eventStep = new NewCancelledEnrollmentWithEventsStep();
        setValuesForMemberFields(eventStep, "mappingReader", mappingReader);
        setValuesForMemberFields(eventStep, "processorObjectFactory", objectFactory);
        setValuesForMemberFields(eventStep, "writer", writer);
        setValuesForMemberFields(eventStep, "stepFactory", stepFactory);
    }

    @Test
    public void shouldReturnStep() {
        String enrLookupTable = "enrollment_lookup_table";
        String programName = "HTS Service";
        String stepName = "New Cancelled Enrollment With Events Step:: ";
        String mappingObj = "";
        String envLookupTable = "patient_enrollment";

        when(mappingReader.getNewCancelledEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable)).thenReturn(jdbcCursorItemReader);
        when(objectFactory.getObject()).thenReturn(processor);
        when(stepFactory.build(stepName, jdbcCursorItemReader, processor, writer)).thenReturn(step);

        Step actual = eventStep.get(enrLookupTable, envLookupTable, programName, mappingObj);

        verify(mappingReader, times(1)).getNewCancelledEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable);
        verify(stepFactory, times(1)).build(stepName, jdbcCursorItemReader, processor, writer);
        assertEquals(step, actual);
    }
}