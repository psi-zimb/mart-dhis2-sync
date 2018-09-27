package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.EventProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.writer.EventWriter;
import lombok.Setter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.EventUtil.resetEventTrackersList;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_EVENT;

@Component
public class EventStep implements StepBuilderContract{

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<EventProcessor> processorObjectFactory;

    @Autowired
    private EventWriter writer;

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private MarkerUtil markerUtil;

    @Setter
    private String enrollmentLookupTable;

    private static final String EVENT_STEP_NAME = "Event Step";

    @Override
    public Step get(String lookupTable, String programName, Object mappingObj) {
        resetEventTrackersList();
        EventUtil.date = markerUtil.getLastSyncedDate(programName, CATEGORY_EVENT);
        return stepFactory.build(EVENT_STEP_NAME,
                mappingReader.getEventReader(lookupTable, programName, enrollmentLookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private EventProcessor getProcessor(Object mappingObj) {
        EventProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }
}
