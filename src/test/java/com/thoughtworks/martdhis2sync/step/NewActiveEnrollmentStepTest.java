package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.NewActiveEnrollmentTasklet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.batch.core.Step;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class NewActiveEnrollmentStepTest {
    @Mock
    private StepFactory stepFactory;

    @Mock
    private NewActiveEnrollmentTasklet tasklet;

    @Mock
    private Step step;

    private NewActiveEnrollmentStep enrollmentStep;

    @Before
    public void setUp() throws Exception {
        enrollmentStep = new NewActiveEnrollmentStep();

        setValuesForMemberFields(enrollmentStep, "stepFactory", stepFactory);
        setValuesForMemberFields(enrollmentStep, "tasklet", tasklet);
    }

    @Test
    public void shouldReturnStep() {
        String stepMessage = "New Active Enrollment Step:: ";
        when(stepFactory.build(stepMessage, tasklet)).thenReturn(step);

        Step actual = enrollmentStep.get();

        assertEquals(step, actual);
        verify(stepFactory, times(1)).build(stepMessage, tasklet);
    }
}