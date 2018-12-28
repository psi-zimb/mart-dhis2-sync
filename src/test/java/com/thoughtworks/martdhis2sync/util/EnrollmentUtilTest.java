package com.thoughtworks.martdhis2sync.util;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class EnrollmentUtilTest {

    @Test
    public void shouldUpdateEventDateWhenTheGivenDateIsGreater() {
        EnrollmentUtil.date = new Date(Long.MIN_VALUE);
        String givenDate = "2018-10-28 12:00:03";

        EnrollmentUtil.updateLatestEnrollmentDateCreated(givenDate);

        assertEquals(givenDate, BatchUtil.getStringFromDate(EnrollmentUtil.date, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }

    @Test
    public void shouldNotUpdateEventDateWhenTheGivenDateIsLesser() {
        String actualDate = "2018-11-12 12:10:10";
        EnrollmentUtil.date = BatchUtil.getDateFromString(actualDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        String givenDate = "2018-10-28 12:00:03";

        EnrollmentUtil.updateLatestEnrollmentDateCreated(givenDate);

        assertEquals(actualDate, BatchUtil.getStringFromDate(EnrollmentUtil.date, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }
}
