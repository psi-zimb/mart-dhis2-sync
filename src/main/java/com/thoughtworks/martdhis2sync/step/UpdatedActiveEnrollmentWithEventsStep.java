package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.processor.UpdatedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.UpdatedActiveEnrollmentWithEventsWriter;
import com.thoughtworks.martdhis2sync.writer.UpdatedEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.checkDates;

@Component
public class UpdatedActiveEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<UpdatedEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private UpdatedActiveEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private static final String STEP_NAME = "Updated Active Enrollment With Events Step:: ";

    public Step get(String enrLookupTable, String envLookupTable, String programName, Object mappingObj,
                    List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String startDate, String endDate) {
        writer.updateLastSyncedDate = checkDates(startDate,endDate) ? true : false;
        return stepFactory.build(STEP_NAME,
                checkDates(startDate,endDate)
                        ? mappingReader.getUpdatedActiveEnrollmentWithEventsReaderWithDateRange(enrLookupTable, programName, envLookupTable, enrollmentsToIgnore, startDate, endDate)
                        : mappingReader.getUpdatedActiveEnrollmentWithEventsReader(enrLookupTable, programName, envLookupTable, enrollmentsToIgnore),
                getProcessor(mappingObj),
                writer);
    }

    private UpdatedEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        UpdatedEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);
        return processor;
    }

}
