package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.ProgramEnrollmentProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.writer.ProgramEnrollmentWriter;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProgramEnrollmentStep implements StepBuilderContract {

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ProgramEnrollmentProcessor processor;

    @Autowired
    private ProgramEnrollmentWriter writer;

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private MarkerUtil markerUtil;

    private static final String PE_STEP_NAME = "Program Enrollment Step";

    @Override
    public Step get(String lookupTable, String programName, Object mappingObj) {
        EnrollmentUtil.resetEnrollmentsList();
        EnrollmentUtil.date = markerUtil.getLastSyncedDate(programName, "enrollment");
        return stepFactory.build(PE_STEP_NAME, mappingReader.getEnrollmentReader(lookupTable, programName), processor, writer);
    }
}
