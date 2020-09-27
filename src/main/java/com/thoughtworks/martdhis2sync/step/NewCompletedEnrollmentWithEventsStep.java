package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.NewEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.NewCompletedEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.checkDates;

@Component
public class NewCompletedEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<NewEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private NewCompletedEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private static final String STEP_NAME = "New Completed Enrollment With Events Step:: ";

    public Step get(String insLookupTable,String enrLookupTable, String envLookupTable, String programName, Object mappingObj, String startDate, String endDate) {

        NewCompletedEnrollmentWithEventsWriter.updateLastSyncedDate = checkDates(startDate, endDate);

        return stepFactory.build(STEP_NAME,
                checkDates(startDate,endDate)
                        ? mappingReader.getNewCompletedEnrollmentWithEventsReaderWithDateRange(insLookupTable,enrLookupTable, programName, envLookupTable, startDate, endDate)
                        : mappingReader.getNewCompletedEnrollmentWithEventsReader(insLookupTable,enrLookupTable, programName, envLookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private NewEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        NewEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }

}
