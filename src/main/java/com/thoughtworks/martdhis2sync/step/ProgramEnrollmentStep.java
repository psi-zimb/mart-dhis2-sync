package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.ProgramEnrollmentProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
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

    private static final String PE_STEP_NAME = "Program Enrollment Step";

    @Override
    public Step get(String lookupTable, String programName, Object mappingObj) {
        EnrollmentUtil.resetEnrollmentsList();

        return stepFactory.build(PE_STEP_NAME, mappingReader.getEnrollmentReader(lookupTable, programName), processor, writer);
    }
}
