package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.ProgramEnrollmentProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.writer.ProgramEnrollmentWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;

import java.util.Date;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_ENROLLMENT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ProgramEnrollmentStepTest {

    @Mock
    private MappingReader mappingReader;

    @Mock
    private JdbcCursorItemReader<Map<String, Object>> jdbcCursorItemReader;

    @Mock
    private ProgramEnrollmentProcessor processor;

    @Mock
    private ProgramEnrollmentWriter writer;

    @Mock
    private TaskletStep step;

    @Mock
    private StepFactory stepFactory;

    @Mock
    private MarkerUtil markerUtil;

    private ProgramEnrollmentStep programEnrollmentStep;

    @Before
    public void setUp() throws Exception {
        programEnrollmentStep = new ProgramEnrollmentStep();
        setValuesForMemberFields(programEnrollmentStep, "mappingReader", mappingReader);
        setValuesForMemberFields(programEnrollmentStep, "processor", processor);
        setValuesForMemberFields(programEnrollmentStep, "writer", writer);
        setValuesForMemberFields(programEnrollmentStep, "stepFactory", stepFactory);
        setValuesForMemberFields(programEnrollmentStep, "markerUtil", markerUtil);
    }

    @Test
    public void shouldReturnStep() {
        String lookupTable = "patient_identifier";
        String programName = "Enrollment Service";
        String stepName = "Program Enrollment Step";
        Date lastSyncedDate = new Date(Long.MIN_VALUE);

        when(mappingReader.getEnrollmentReader(lookupTable, programName)).thenReturn(jdbcCursorItemReader);
        when(stepFactory.build(stepName, jdbcCursorItemReader, processor, writer)).thenReturn(step);
        when(markerUtil.getLastSyncedDate(programName, CATEGORY_ENROLLMENT)).thenReturn(lastSyncedDate);

        Step actual = programEnrollmentStep.get(lookupTable, programName, new Object());

        verify(mappingReader, times(1)).getEnrollmentReader(lookupTable, programName);
        verify(stepFactory, times(1)).build(stepName, jdbcCursorItemReader, processor, writer);
        verify(markerUtil, times(1)).getLastSyncedDate(programName, CATEGORY_ENROLLMENT);
        assertEquals(step, actual);
    }
}