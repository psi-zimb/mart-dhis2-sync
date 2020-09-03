package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.writer.NewCancelledEnrollmentTasklet;
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
public class NewCancelledEnrollmentStepTest {
    @Mock
    private StepFactory stepFactory;

    @Mock
    private NewCancelledEnrollmentTasklet newCancelledEnrollmentTasklet;

    @Mock
    private Step step;

    private NewCancelledEnrollmentStep newCancelledEnrollmentStep;

    @Before
    public void setUp() throws Exception {
        newCancelledEnrollmentStep = new NewCancelledEnrollmentStep();

        setValuesForMemberFields(newCancelledEnrollmentStep, "stepFactory", stepFactory);
        setValuesForMemberFields(newCancelledEnrollmentStep, "newCancelledEnrollmentTasklet", newCancelledEnrollmentTasklet);
    }

    @Test
    public void shouldReturnStep() {
        String stepMessage = "New Cancelled Enrollment Step:: ";
        when(stepFactory.build(stepMessage, newCancelledEnrollmentTasklet)).thenReturn(step);

        Step actual = newCancelledEnrollmentStep.get();

        assertEquals(step, actual);
        verify(stepFactory, times(1)).build(stepMessage, newCancelledEnrollmentTasklet);
    }
}