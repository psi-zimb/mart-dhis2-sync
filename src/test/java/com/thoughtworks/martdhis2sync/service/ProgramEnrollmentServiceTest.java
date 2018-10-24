package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.step.ProgramEnrollmentStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.SyncFailedException;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ProgramEnrollmentServiceTest {

    @Mock
    private JobService jobService;

    @Mock
    private ProgramEnrollmentStep enrollmentStep;

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

        programEnrollmentService.triggerJob(programName, user, lookUpTable);

        verify(jobService, times(1)).triggerJob(programName, user, lookUpTable, jobName, enrollmentStep, null);
    }

    @Test(expected = SyncFailedException.class)
    public void shouldThrowSyncFailedException() throws Exception {
        String lookUpTable = "patient_identifier";
        String service = "serviceName";
        String user = "Admin";
        String jobName = "Sync Program Enrollment";

        doThrow(SyncFailedException.class).when(jobService)
                .triggerJob(service, user, lookUpTable, jobName, enrollmentStep, null);

        programEnrollmentService.triggerJob(service, user, lookUpTable);
    }
}
