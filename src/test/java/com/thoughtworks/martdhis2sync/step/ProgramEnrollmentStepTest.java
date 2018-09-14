package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.ProgramEnrollmentProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
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

import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ProgramEnrollmentStepTest {

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
    private ProgramEnrollmentProcessor processor;

    @Mock
    private TaskletStep step;


    private ProgramEnrollmentStep programEnrollmentStep;

    @Before
    public void setUp() throws Exception {
        programEnrollmentStep = new ProgramEnrollmentStep();
        setValuesForMemberFields(programEnrollmentStep, "stepBuilderFactory", stepBuilderFactory);
        setValuesForMemberFields(programEnrollmentStep, "mappingReader", mappingReader);
        setValuesForMemberFields(programEnrollmentStep, "processor", processor);
    }

    @Test
    public void shouldReturnStep() {
        String lookupTable = "patient_identifier";

        when(stepBuilderFactory.get("ProgramEnrollmentStep")).thenReturn(stepBuilder);
        when(stepBuilder.chunk(500)).thenReturn(simpleStepBuilder);
        when(mappingReader.getEnrollmentReader(lookupTable)).thenReturn(jdbcCursorItemReader);
        when(simpleStepBuilder.reader(jdbcCursorItemReader)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.processor(processor)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.build()).thenReturn(step);

        programEnrollmentStep.get(lookupTable);

        verify(stepBuilderFactory, times(1)).get("ProgramEnrollmentStep");
        verify(stepBuilder, times(1)).chunk(500);
        verify(mappingReader, times(1)).getEnrollmentReader(lookupTable);
        verify(simpleStepBuilder, times(1)).reader(jdbcCursorItemReader);
        verify(simpleStepBuilder, times(1)).processor(processor);
        verify(simpleStepBuilder, times(1)).build();
    }

}