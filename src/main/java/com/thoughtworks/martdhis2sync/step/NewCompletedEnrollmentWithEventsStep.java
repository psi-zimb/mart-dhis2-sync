package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.NewEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.NewActiveAndCompletedEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NewCompletedEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<NewEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private NewActiveAndCompletedEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private static final String STEP_NAME = "New Completed Enrollment With Events Step:: ";

    public Step get(String enrLookupTable, String envLookupTable, String programName, Object mappingObj) {

        return stepFactory.build(STEP_NAME,
                mappingReader.getNewCompletedEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private NewEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        NewEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }

}
