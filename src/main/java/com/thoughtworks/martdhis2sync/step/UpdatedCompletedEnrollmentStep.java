package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.UpdatedCompletedEnrollmentTasklet;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpdatedCompletedEnrollmentStep {
    
    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private UpdatedCompletedEnrollmentTasklet tasklet;

    private static final String STEP_NAME = "Updated Completed Enrollment Step:: ";

    public Step get() {
        return stepFactory.build(STEP_NAME, tasklet);
    }
}
