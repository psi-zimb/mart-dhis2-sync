package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.processor.UpdatedCompletedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.UpdatedCompletedEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class UpdatedActiveEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<UpdatedCompletedEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private UpdatedCompletedEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private static final String STEP_NAME = "Updated Active Enrollment With Events Step:: ";

    public Step get(String enrLookupTable, String envLookupTable, String programName, Object mappingObj,
                    List<EnrollmentAPIPayLoad> enrollmentsToIgnore) {
        return stepFactory.build(STEP_NAME,
                mappingReader.getUpdatedActiveEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable, enrollmentsToIgnore),
                getProcessor(mappingObj),
                writer);
    }

    private UpdatedCompletedEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        UpdatedCompletedEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }

}