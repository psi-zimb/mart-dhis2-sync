package com.thoughtworks.martdhis2sync.util;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class EnrollmentUtilTest {

    @Test
    public void shouldUpdateCompletedEnrollmentDateWhenTheGivenDateIsGreater() throws Exception {
        EnrollmentUtil.updatedCompletedDate = new Date(Long.MIN_VALUE);
        String givenDate = "2018-10-28 12:00:03";

        EnrollmentUtil.updateLatestEnrollmentDateCreated(givenDate,  MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT);

        assertEquals(givenDate, BatchUtil.getStringFromDate(EnrollmentUtil.updatedCompletedDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }

    @Test
    public void shouldNotUpdateCompletedEnrollmentDateWhenTheGivenDateIsLesser() throws Exception {
        String actualDate = "2018-11-12 12:10:10";
        EnrollmentUtil.updatedCompletedDate = BatchUtil.getDateFromString(actualDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        String givenDate = "2018-10-28 12:00:03";

        EnrollmentUtil.updateLatestEnrollmentDateCreated(givenDate, MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT);

        assertEquals(actualDate, BatchUtil.getStringFromDate(EnrollmentUtil.updatedCompletedDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }

    @Test
    public void shouldUpdateActiveEnrollmentDateWhenTheGivenDateIsGreater() throws Exception {
        EnrollmentUtil.updatedActiveDate = new Date(Long.MIN_VALUE);
        String givenDate = "2018-10-28 12:00:03";

        EnrollmentUtil.updateLatestEnrollmentDateCreated(givenDate, MarkerUtil.CATEGORY_UPDATED_ACTIVE_ENROLLMENT);

        assertEquals(givenDate, BatchUtil.getStringFromDate(EnrollmentUtil.updatedActiveDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }

    @Test
    public void shouldNotUpdateActiveEnrollmentDateWhenTheGivenDateIsLesser() throws Exception {
        String actualDate = "2018-11-12 12:10:10";
        EnrollmentUtil.updatedActiveDate = BatchUtil.getDateFromString(actualDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        String givenDate = "2018-10-28 12:00:03";

        EnrollmentUtil.updateLatestEnrollmentDateCreated(givenDate, MarkerUtil.CATEGORY_UPDATED_ACTIVE_ENROLLMENT);

        assertEquals(actualDate, BatchUtil.getStringFromDate(EnrollmentUtil.updatedActiveDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }
}
