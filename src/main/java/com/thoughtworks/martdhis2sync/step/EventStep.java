package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.EventProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.writer.EventWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_ENROLLMENT;
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

    private static final String TEI_STEP_NAME = "Event Step";

    @Override
    public Step get(String lookupTable, String programName, Object mappingObj) {
        EventUtil.date = markerUtil.getLastSyncedDate(programName, CATEGORY_EVENT);
        return stepFactory.build(TEI_STEP_NAME,
                mappingReader.getEventReader(lookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private EventProcessor getProcessor(Object mappingObj) {
        EventProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }
}
