package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.ActiveEnrollmentTasklet;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ActiveEnrollmentStep {

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private ActiveEnrollmentTasklet tasklet;

    private static final String NCE_STEP_NAME = "Active Enrollment Step:: ";

    public Step get() {
        return stepFactory.build(NCE_STEP_NAME, tasklet);
    }
}
