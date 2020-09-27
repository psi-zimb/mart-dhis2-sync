package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.processor.UpdatedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.UpdatedCancelledEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.checkDates;

@Component
public class UpdatedCancelledEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<UpdatedEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private UpdatedCancelledEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private static final String STEP_NAME = "Updated Cancelled Enrollment With Events Step:: ";

    public Step get(String insLookupTable,String enrLookupTable, String envLookupTable, String programName, Object mappingObj, List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String startDate, String endDate) {
        UpdatedCancelledEnrollmentWithEventsWriter.updateLastSyncedDate = checkDates(startDate, endDate);
        return stepFactory.build(STEP_NAME,
                checkDates(startDate,endDate)
                        ? mappingReader.getUpdatedCancelledEnrollmentWithEventsReaderWithDateRange(insLookupTable,enrLookupTable,programName,envLookupTable,enrollmentsToIgnore,startDate,endDate)
                        : mappingReader.getUpdatedCancelledEnrollmentWithEventsReader(insLookupTable,enrLookupTable, programName, envLookupTable, enrollmentsToIgnore),
                getProcessor(mappingObj),
                writer);
    }

    private UpdatedEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        UpdatedEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }
}
