package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.UpdatedCompletedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.UpdatedCompletedEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpdatedCompletedEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<UpdatedCompletedEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private UpdatedCompletedEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private static final String STEP_NAME = "Updated Completed Enrollment With Events Step:: ";

    public Step get(String enrLookupTable, String envLookupTable, String programName, Object mappingObj) {
        return stepFactory.build(STEP_NAME,
                mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private UpdatedCompletedEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        UpdatedCompletedEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }

}