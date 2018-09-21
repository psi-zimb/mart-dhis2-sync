package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.EventProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.EventWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    private static final String TEI_STEP_NAME = "Event Step";

    @Override
    public Step get(String lookupTable, String programName, Object mappingObj) {

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
