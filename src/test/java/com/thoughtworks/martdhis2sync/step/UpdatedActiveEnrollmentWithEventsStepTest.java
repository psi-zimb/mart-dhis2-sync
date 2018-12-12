package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.UpdatedCompletedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.UpdatedCompletedEnrollmentWithEventsWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.Step;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.ObjectFactory;

import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class UpdatedActiveEnrollmentWithEventsStepTest {
    @Mock
    private MappingReader mappingReader;

    @Mock
    private JdbcCursorItemReader<Map<String, Object>> jdbcCursorItemReader;

    @Mock
    private ObjectFactory<UpdatedCompletedEnrollmentWithEventsProcessor> objectFactory;

    @Mock
    private UpdatedCompletedEnrollmentWithEventsProcessor processor;

    @Mock
    private UpdatedCompletedEnrollmentWithEventsWriter writer;

    @Mock
    private Step step;

    @Mock
    private StepFactory stepFactory;

    private UpdatedActiveEnrollmentWithEventsStep eventStep;

    @Before
    public void setUp() throws Exception {
        eventStep = new UpdatedActiveEnrollmentWithEventsStep();
        setValuesForMemberFields(eventStep, "mappingReader", mappingReader);
        setValuesForMemberFields(eventStep, "processorObjectFactory", objectFactory);
        setValuesForMemberFields(eventStep, "writer", writer);
        setValuesForMemberFields(eventStep, "stepFactory", stepFactory);
    }

    @Test
    public void shouldReturnStep() {
        String enrLookupTable = "enrollment_lookup_table";
        String programName = "HTS Service";
        String stepName = "Updated Active Enrollment With Events Step:: ";
        String mappingObj = "";
        String envLookupTable = "patient_enrollment";

        when(mappingReader.getUpdatedActiveEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable)).thenReturn(jdbcCursorItemReader);
        when(objectFactory.getObject()).thenReturn(processor);
        when(stepFactory.build(stepName, jdbcCursorItemReader, processor, writer)).thenReturn(step);

        Step actual = eventStep.get(enrLookupTable, envLookupTable, programName, mappingObj);

        verify(mappingReader, times(1)).getUpdatedActiveEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable);
        verify(stepFactory, times(1)).build(stepName, jdbcCursorItemReader, processor, writer);
        assertEquals(step, actual);
    }

}