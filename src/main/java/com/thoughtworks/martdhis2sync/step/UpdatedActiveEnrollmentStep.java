package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.UpdatedActiveEnrollmentTasklet;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpdatedActiveEnrollmentStep {

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private UpdatedActiveEnrollmentTasklet tasklet;

    private static final String STEP_NAME = "Updated Active Enrollment Step:: ";

    public Step get() {
        return stepFactory.build(STEP_NAME, tasklet);
    }
}
