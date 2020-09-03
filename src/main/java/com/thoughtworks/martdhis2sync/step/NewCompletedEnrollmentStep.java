package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.NewCompletedEnrollmentTasklet;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NewCompletedEnrollmentStep {

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private NewCompletedEnrollmentTasklet newCompletedEnrollmentTasklet;

    private static final String NCE_STEP_NAME = "New Completed Enrollment Step:: ";

    public Step get() {
        return stepFactory.build(NCE_STEP_NAME, newCompletedEnrollmentTasklet);
    }
}
