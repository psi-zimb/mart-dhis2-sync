package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.TrackedEntityInstanceProcessor;
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
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;

import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class StepFactoryTest {

    @Mock
    private StepBuilderFactory stepBuilderFactory;

    @Mock
    private StepBuilder stepBuilder;

    @Mock
    private SimpleStepBuilder<Object, Object> simpleStepBuilder;

    @Mock
    private JdbcCursorItemReader<Map<String, Object>> jdbcCursorItemReader;

    @Mock
    private TrackedEntityInstanceProcessor processor;

    @Mock
    private TrackedEntityInstanceWriter writer;

    @Mock
    private TaskletStep step;

    @Mock
    private Tasklet tasklet;

    @Mock
    private TaskletStepBuilder taskletStepBuilder;

    private StepFactory stepFactory;

    @Before
    public void setUp() throws Exception {
        stepFactory = new StepFactory();
        setValuesForMemberFields(stepFactory, "stepBuilderFactory", stepBuilderFactory);
    }

    @Test
    public void shouldReturnStep() {
        String stepName = "Step Name";

        when(stepBuilderFactory.get(stepName)).thenReturn(stepBuilder);
        when(stepBuilder.chunk(500)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.reader(jdbcCursorItemReader)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.processor(processor)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.writer(writer)).thenReturn(simpleStepBuilder);
        when(simpleStepBuilder.build()).thenReturn(step);

        stepFactory.build(stepName, jdbcCursorItemReader, processor, writer);

        verify(stepBuilderFactory, times(1)).get(stepName);
        verify(stepBuilder, times(1)).chunk(500);
        verify(simpleStepBuilder, times(1)).reader(jdbcCursorItemReader);
        verify(simpleStepBuilder, times(1)).processor(processor);
        verify(simpleStepBuilder, times(1)).writer(writer);
        verify(simpleStepBuilder, times(1)).build();
    }

    @Test
    public void shouldReturnTaskletStep() {
        String stepName = "Step Name";

        when(stepBuilderFactory.get(stepName)).thenReturn(stepBuilder);
        when(stepBuilder.tasklet(tasklet)).thenReturn(taskletStepBuilder);
        when(taskletStepBuilder.build()).thenReturn(step);

        stepFactory.build(stepName, tasklet);

        verify(stepBuilderFactory, times(1)).get(stepName);
        verify(stepBuilder, times(1)).tasklet(tasklet);
        verify(taskletStepBuilder, times(1)).build();
    }
}
