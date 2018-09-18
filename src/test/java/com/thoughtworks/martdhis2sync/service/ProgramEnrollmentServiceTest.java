package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.step.ProgramEnrollmentStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ProgramEnrollmentService.class)
@PowerMockIgnore("javax.management.*")
public class ProgramEnrollmentServiceTest {

    @Mock
    private JobService jobService;

    @Mock
    private ProgramEnrollmentStep enrollmentStep;

    @Mock
    private Object object;

    private ProgramEnrollmentService programEnrollmentService;

    @Before
    public void setUp() throws Exception {
        programEnrollmentService = new ProgramEnrollmentService();
        setValuesForMemberFields(programEnrollmentService, "programEnrollmentStep", enrollmentStep);
        setValuesForMemberFields(programEnrollmentService, "jobService", jobService);
    }

    @Test
    public void shouldTriggerTheJob() throws Exception {
        String jobName = "Sync Program Enrollment";

        String lookUpTable = "patient_program_enrollment";
        String user = "testUser";
        String programName = "Enrollment Service";

        whenNew(Object.class).withNoArguments().thenReturn(object);
        programEnrollmentService.triggerJob(programName, user, lookUpTable);

        verify(jobService, times(1)).triggerJob(programName, user, lookUpTable, jobName, enrollmentStep, object);
    }
}
