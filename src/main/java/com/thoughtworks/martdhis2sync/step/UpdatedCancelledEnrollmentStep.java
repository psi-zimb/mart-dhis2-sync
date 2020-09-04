package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.UpdatedCancelledEnrollmentTasklet;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UpdatedCancelledEnrollmentStep {

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private UpdatedCancelledEnrollmentTasklet updatedCancelledEnrollmentTasklet;

    private static final String STEP_NAME = "Updated Cancelled Enrollment Step:: ";

    public Step get() {
        return stepFactory.build(STEP_NAME, updatedCancelledEnrollmentTasklet);
    }
}
