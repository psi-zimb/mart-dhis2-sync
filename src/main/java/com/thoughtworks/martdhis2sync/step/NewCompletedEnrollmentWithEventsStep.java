package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.NewCompletedEnrollmentWithEventsProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.writer.NewCompletedEnrollmentWithEventsWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_ENROLLMENT;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_EVENT;

@Component
public class NewCompletedEnrollmentWithEventsStep {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ObjectFactory<NewCompletedEnrollmentWithEventsProcessor> processorObjectFactory;

    @Autowired
    private NewCompletedEnrollmentWithEventsWriter writer;

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private MarkerUtil markerUtil;

    private static final String NCE_STEP_NAME = "New Completed Enrollment With Events Step:: ";

    public Step get(String enrLookupTable, String envLookupTable, String programName, Object mappingObj) {
        EnrollmentUtil.date = markerUtil.getLastSyncedDate(programName, CATEGORY_ENROLLMENT);
        EventUtil.date = markerUtil.getLastSyncedDate(programName, CATEGORY_EVENT);
        return stepFactory.build(NCE_STEP_NAME,
                mappingReader.getNewCompletedEnrollmentReader(enrLookupTable, programName, envLookupTable),
                getProcessor(mappingObj),
                writer);
    }

    private NewCompletedEnrollmentWithEventsProcessor getProcessor(Object mappingObj) {
        NewCompletedEnrollmentWithEventsProcessor processor = processorObjectFactory.getObject();
        processor.setMappingObj(mappingObj);

        return processor;
    }

}
