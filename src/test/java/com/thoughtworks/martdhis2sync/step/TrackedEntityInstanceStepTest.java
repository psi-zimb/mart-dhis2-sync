package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.TrackedEntityInstanceProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import com.thoughtworks.martdhis2sync.writer.TrackedEntityInstanceWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.ObjectFactory;

import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_INSTANCE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class TrackedEntityInstanceStepTest {

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

    @Mock
    private MarkerUtil markerUtil;

    @Mock
    private StepFactory stepFactory;

    private TrackedEntityInstanceStep teiStep;

    @Before
    public void setUp() throws Exception {
        teiStep = new TrackedEntityInstanceStep();
        setValuesForMemberFields(teiStep, "mappingReader", mappingReader);
        setValuesForMemberFields(teiStep, "processorObjectFactory", processorObjectFactory);
        setValuesForMemberFields(teiStep, "writer", writer);
        setValuesForMemberFields(teiStep, "markerUtil", markerUtil);
        setValuesForMemberFields(teiStep, "stepFactory", stepFactory);
    }

    @Test
    public void shouldReturnStep() {
        String lookupTable = "patient_identifier";
        Object mappingObj = "";
        String programName = "TB Service";
        String stepName = "Tracked Entity Step";
        List<String> searchableAttributes = Collections.singletonList("UIC");
        List<String> comparableAttributes = Arrays.asList("patient_id", "prepID");

        when(markerUtil.getLastSyncedDate(programName, CATEGORY_INSTANCE)).thenReturn(new Date(Long.MIN_VALUE));
        when(mappingReader.getInstanceReader(anyString(), anyString())).thenReturn(jdbcCursorItemReader);
        when(processorObjectFactory.getObject()).thenReturn(processor);
        when(stepFactory.build(stepName, jdbcCursorItemReader, processor, writer)).thenReturn(step);

        teiStep.get(lookupTable, programName, mappingObj, searchableAttributes, comparableAttributes);

        verify(mappingReader, times(1)).getInstanceReader(anyString(), anyString());
        verify(processorObjectFactory, times(1)).getObject();
        verify(stepFactory, times(1)).build(stepName, jdbcCursorItemReader, processor, writer);
        verify(processor, times(1)).setSearchableAttributes(searchableAttributes);
        verify(processor, times(1)).setComparableAttributes(comparableAttributes);

        String expectedUTC = "Sun Dec 02 16:47:04 UTC 292269055";
        String expectedIST = "Sun Dec 02 22:17:04 IST 292269055";
        String actual = TEIUtil.date.toString();
        if(actual.contains("UTC")) {
            assertEquals(expectedUTC, actual);
        }
        if(actual.contains("IST")) {
            assertEquals(expectedIST, actual);
        }
    }
}