package com.thoughtworks.martdhis2sync.util;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class BatchUtil {

    private static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATEFORMAT_WITH_24HR_TIME = "yyyy-MM-dd kk:mm:ss";
    public static final String DATEFORMAT_WITHOUT_TIME = "yyyy-MM-dd";

    public static String convertResourceOutputToString(Resource resource) throws IOException {
        return IOUtils.toString(resource.getInputStream());
    }

    public static String getUnquotedString(String string) {
        return StringUtils.replace(string, "\"", "");
    }

    public static String GetUTCDateTimeAsString() {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATEFORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date());
    }

    public static String getStringFromDate(Date date, String format) {
        SimpleDateFormat outputFormat = new SimpleDateFormat(format);
        return outputFormat.format(date);
    }

    public static Date getDateFromString(String date, String format) {
        SimpleDateFormat outputFormat = new SimpleDateFormat(format);
        try {
            return outputFormat.parse(date);
        } catch (ParseException ignored) {

        }
        return new Date(Long.MIN_VALUE);
    }

    public static String getFormattedDateString(String date, String existingFormat, String expectedFormat) {
        return getStringFromDate(getDateFromString(date, existingFormat), expectedFormat);
    }
}
