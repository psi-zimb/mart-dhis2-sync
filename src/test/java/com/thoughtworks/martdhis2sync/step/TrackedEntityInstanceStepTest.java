package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.TrackedEntityInstanceProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.TrackedEntityInstanceWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.ObjectFactory;

import java.util.Date;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class TrackedEntityInstanceStepTest {

    @Mock
    private StepBuilderFactory stepBuilderFactory;

    @Mock
    private StepBuilder stepBuilder;

    @Mock
    private SimpleStepBuilder<Object, Object> simpleStepBuilder;

    @Mock
    private MappingReader mappingReader;

    @Mock
    private JdbcCursorItemReader<Map<String, Object>> jdbcCursorItemReader;

    @Mock
    private ObjectFactory<TrackedEntityInstanceProcessor> processorObjectFactory;

    @Mock
    private TrackedEntityInstanceProcessor processor;

    @Mock
    private TrackedEntityInstanceWriter writer;

    @Mock
    private TaskletStep step;


    private TrackedEntityInstanceStep teiStep;
    private Date syncedDate = new Date();

    @Before
    public void setUp() throws Exception {
        teiStep = new TrackedEntityInstanceStep();
        setValuesForMemberFields(teiStep, "stepBuilderFactory", stepBuilderFactory);
        setValuesForMemberFields(teiStep, "mappingReader", mappingReader);
        setValuesForMemberFields(teiStep, "processorObjectFactory", processorObjectFactory);
        setValuesForMemberFields(teiStep, "writer", writer);
    }

    @Test
    public void shouldReturnStep() throws Exception {
        String lookupTable = "patient_identifier";
        Object mappingObj = "";
        String programName = "TB Service";

        when(stepBuilderFactory.get("TrackedEntityInstanceStep")).thenReturn(stepBuilder);
        when(stepBuilder.chunk(500)).thenReturn(simpleStepBuilder);
        when(mappingReader.get(anyString(), anyString())).thenReturn(jdbcCursorItemReader);
        when(simpleStepBuilder.reader(jdbcCursorItemReader)).thenReturn(simpleStepBuilder);
        when(processorObjectFactory.getObject()).thenReturn(processor);
        when(simpleStepBuilder.processor(processor)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.writer(writer)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.build()).thenReturn(step);

        teiStep.get(lookupTable, mappingObj, programName);

        verify(stepBuilderFactory, times(1)).get("TrackedEntityInstanceStep");
        verify(stepBuilder, times(1)).chunk(500);
        verify(mappingReader, times(1)).get(anyString(), anyString());
        verify(simpleStepBuilder, times(1)).reader(jdbcCursorItemReader);
        verify(processorObjectFactory, times(1)).getObject();
        verify(simpleStepBuilder, times(1)).processor(processor);
        verify(simpleStepBuilder, times(1)).writer(writer);
        verify(simpleStepBuilder, times(1)).build();
    }
}