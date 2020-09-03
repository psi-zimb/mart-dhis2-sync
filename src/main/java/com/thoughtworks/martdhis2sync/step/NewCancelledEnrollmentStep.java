package com.thoughtworks.martdhis2sync.step;


import com.thoughtworks.martdhis2sync.writer.NewCancelledEnrollmentTasklet;
import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NewCancelledEnrollmentStep {

    @Autowired
    private StepFactory stepFactory;

    @Autowired
    private NewCancelledEnrollmentTasklet newCancelledEnrollmentTasklet;

    private static final String NCE_STEP_NAME = "New Cancelled Enrollment Step:: ";

    public Step get() {
        return stepFactory.build(NCE_STEP_NAME, newCancelledEnrollmentTasklet);
    }
}
