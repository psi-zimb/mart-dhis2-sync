package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.processor.UpdatedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.UpdatedCancelledEnrollmentWithEventsWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.ObjectFactory;

import java.util.ArrayList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class UpdatedCancelledEnrollmentWithEventsStepTest {
    @Mock
    private MappingReader mappingReader;

    @Mock
    private ObjectFactory<UpdatedEnrollmentWithEventsProcessor> objectFactory;

    @Mock
    private UpdatedEnrollmentWithEventsProcessor processor;

    @Mock
    private UpdatedCancelledEnrollmentWithEventsWriter writer;

    @Mock
    private StepFactory stepFactory;

    private UpdatedCancelledEnrollmentWithEventsStep eventStep;

    @Before
    public void setUp() throws Exception {
        eventStep = new UpdatedCancelledEnrollmentWithEventsStep();
        setValuesForMemberFields(eventStep, "mappingReader", mappingReader);
        setValuesForMemberFields(eventStep, "processorObjectFactory", objectFactory);
        setValuesForMemberFields(eventStep, "writer", writer);
        setValuesForMemberFields(eventStep, "stepFactory", stepFactory);
    }

    @Test
    public void whenCreatingAStepWithParametersThenShouldGetMappingReaderWithSameParameters() {
        String enrLookupTable = "enrollment_lookup_table";
        String insLookupTable = "enrollment_lookup_table";
        String programName = "HTS Service";
        String mappingObj = "";
        String envLookupTable = "patient_event";
        List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();

        when(objectFactory.getObject()).thenReturn(processor);
        eventStep.get(insLookupTable,enrLookupTable, envLookupTable, programName, mappingObj, enrollmentsToIgnore,"", "");

        verify(mappingReader, times(1)).getUpdatedCancelledEnrollmentWithEventsReader(insLookupTable,enrLookupTable, programName, envLookupTable, enrollmentsToIgnore);
    }

    @Test
    public void whenCreatingAStepWithParametersThenShouldCallStepFactoryWithSameParameters() {
        String enrLookupTable = "enrollment_lookup_table";
        String insLookupTable = "enrollment_lookup_table";
        String programName = "HTS Service";
        String mappingObj = "";
        String envLookupTable = "patient_event";
        List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();

        when(objectFactory.getObject()).thenReturn(processor);
        eventStep.get(insLookupTable,enrLookupTable, envLookupTable, programName, mappingObj, enrollmentsToIgnore,"", "");

        verify(stepFactory, times(1)).build(anyString(), any(), any(), any());
    }
}