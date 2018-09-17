package com.thoughtworks.martdhis2sync.util;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class BatchUtil {

    private static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";

    public static String convertResourceOutputToString(Resource resource) throws IOException {
        return IOUtils.toString(resource.getInputStream());
    }

    public static String GetUTCdatetimeAsString()
    {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATEFORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return  sdf.format(new Date());
    }

    public static String getStringFromDate(Date date) {
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        return outputFormat.format(date);
    }

    public static Date getDateFromString(String date) {
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        try {
            return outputFormat.parse(date);
        } catch (ParseException ignored) {

        }
        return new Date(Long.MIN_VALUE);
    }
}
