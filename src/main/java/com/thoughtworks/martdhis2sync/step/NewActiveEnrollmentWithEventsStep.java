package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.NewEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.writer.NewActiveEnrollmentWithEventsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.checkDates;

@Component
public class NewActiveEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<NewEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private NewActiveEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String STEP_NAME = "New Active Enrollment With Events Step:: ";

    public Step get(String enrLookupTable,String insLookupTable, String envLookupTable, String programName, Object mappingObj, String startDate, String endDate) {

        NewActiveEnrollmentWithEventsWriter.updateLastSyncedDate = checkDates(startDate, endDate);
        logger.info("startDate ->" + startDate);
        logger.info("endDate -> " + endDate);
        logger.info("result NewActiveEnrollmentWithEventsStep -> " + checkDates(startDate,endDate));
        return stepFactory.build(STEP_NAME,
                checkDates(startDate,endDate)
                        ? mappingReader.getNewActiveEnrollmentWithEventsReaderWithDateRange(enrLookupTable, programName, envLookupTable, startDate, endDate)
                        : mappingReader.getNewActiveEnrollmentWithEventsReader(insLookupTable,enrLookupTable, programName, envLookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private NewEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        NewEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }

}
